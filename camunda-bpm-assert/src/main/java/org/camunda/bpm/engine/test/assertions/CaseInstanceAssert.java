package org.camunda.bpm.engine.test.assertions;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.MapAssert;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.history.HistoricActivityInstanceQuery;
import org.camunda.bpm.engine.history.HistoricCaseInstanceQuery;
import org.camunda.bpm.engine.history.HistoricDetailQuery;
import org.camunda.bpm.engine.history.HistoricTaskInstanceQuery;
import org.camunda.bpm.engine.history.HistoricVariableInstanceQuery;
import org.camunda.bpm.engine.repository.CaseDefinitionQuery;
import org.camunda.bpm.engine.runtime.CaseInstance;
import org.camunda.bpm.engine.runtime.CaseInstanceQuery;
import org.camunda.bpm.engine.runtime.Execution;
import org.camunda.bpm.engine.runtime.ExecutionQuery;
import org.camunda.bpm.engine.runtime.JobQuery;
import org.camunda.bpm.engine.runtime.VariableInstanceQuery;
import org.camunda.bpm.engine.task.TaskQuery;

import java.util.Arrays;
import java.util.Map;

/**
 * Assertions for a {@link org.camunda.bpm.engine.runtime.CaseInstance}
 *
 * Copied from {@link org.camunda.bpm.engine.test.assertions.ProcessInstanceAssert}.
 *
 * @author Malte Sörensen <malte.soerensen@holisticon.de>
 */
public class CaseInstanceAssert extends AbstractProcessAssert<CaseInstanceAssert, CaseInstance> {

  protected CaseInstanceAssert(final ProcessEngine engine, final CaseInstance actual) {
    super(engine, actual, CaseInstanceAssert.class);
  }

  protected CaseInstanceAssert(final ProcessEngine engine, final CaseInstance actual, Class<?> selfType) {
    super(engine, actual, selfType);
  }

  protected static CaseInstanceAssert assertThat(final ProcessEngine engine, final CaseInstance actual) {
    return new CaseInstanceAssert(engine, actual);
  }

  @Override
  protected CaseInstance getCurrent() {
    return caseInstanceQuery().singleResult();
  }

  @Override
  protected String toString(CaseInstance processInstance) {
    return processInstance != null ?
      String.format("actual %s {" +
        "id='%s', " +
        "caseDefinitionId='%s', " +
        "businessKey='%s'" +
        "}",
        CaseInstance.class.getSimpleName(),
        processInstance.getId(),
        processInstance.getCaseDefinitionId(),
        processInstance.getBusinessKey())
      : null;
  }

  /**
   * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} holds one or
   * more process variables with the specified names.
   *
   * @param   names the names of the process variables expected to exist. In
   *          case no variable name is given, the existence of at least one
   *          variable will be verified.
   * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
   */
  public CaseInstanceAssert hasVariables(final String... names) {
    return hasVars(names);
  }

  /**
   * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} holds no
   * process variables at all.
   *
   * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
   */
  public CaseInstanceAssert hasNoVariables() {
    return hasVars(null);
  }

  private CaseInstanceAssert hasVars(final String[] names) {
    boolean shouldHaveVariables = names != null;
    boolean shouldHaveSpecificVariables = names != null && names.length > 0;

    CaseInstance current = getExistingCurrent();
    Map<String, Object> vars = caseService().getVariables(getExistingCurrent().getCaseInstanceId());
    MapAssert<String, Object> assertion = (MapAssert<String, Object>) Assertions.assertThat(vars)
      .overridingErrorMessage("Expecting %s to hold " +
        (shouldHaveVariables ? "process variables" + (shouldHaveSpecificVariables ? " %s, " : ", ") : "no variables at all, ") +
        "instead we found it to hold " + (vars.isEmpty() ? "no variables at all." : "the variables %s."),
        toString(current), shouldHaveSpecificVariables ? Arrays.asList(names) : vars.keySet(), vars.keySet()
      );
    if (shouldHaveVariables) {
      if (shouldHaveSpecificVariables) {
        assertion.containsKeys(names);
      } else {
        assertion.isNotEmpty();
      }
    } else {
      assertion.isEmpty();
    }
    return this;
  }

  /**
   * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} is ended.
   *
   * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
   */
  public CaseInstanceAssert isEnded() {
    isNotNull();
    final String message = "Expecting %s to be ended, but it is not!. (Please " +
      "make sure you have set the history service of the engine to at least " +
      "'activity' or a higher level before making use of this assertion!)";
    Assertions.assertThat(processInstanceQuery().singleResult())
      .overridingErrorMessage(message,
        toString(actual))
      .isNull();
    Assertions.assertThat(historicCaseInstanceQuery().singleResult())
      .overridingErrorMessage(message,
        toString(actual))
      .isNotNull();
    return this;
  }

  /**
   * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} is currently
   * disabled.
   *
   * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
   */
  public CaseInstanceAssert isDisabled() {
    CaseInstance current = getExistingCurrent();
    Assertions.assertThat(current.isDisabled())
      .overridingErrorMessage("Expecting %s to be disabled, but it is not!",
        toString(actual))
      .isTrue();
    return this;
  }

  /**
   * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} is not ended.
   *
   * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
   */
  public CaseInstanceAssert isNotEnded() {
    CaseInstance current = getExistingCurrent();
    Assertions.assertThat(current)
      .overridingErrorMessage("Expecting %s not to be ended, but it is!",
        toString(current))
      .isNotNull();
    return this;
  }

  /**
   * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} is currently active.
   *
   * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
   */
  public CaseInstanceAssert isActive() {
    CaseInstance current = getExistingCurrent();
    isStarted();
    isNotEnded();
    Assertions.assertThat(current.isActive())
      .overridingErrorMessage("Expecting %s to be active, but it is not!",
        toString(current))
      .isTrue();
    return this;
  }

	/**
	 * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} is completed.
	 *
	 * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
	 */
	public CaseInstanceAssert isCompleted() {
		CaseInstance current = getExistingCurrent();
		isStarted();
		isNotEnded();
		Assertions.assertThat(current.isCompleted())
				.overridingErrorMessage("Expecting %s to be active, but it is!",
						toString(current))
				.isFalse();
		return this;
	}

	/**
	 * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} is available.
	 *
	 * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
	 */
	public CaseInstanceAssert isAvailable() {
		CaseInstance current = getExistingCurrent();
		isStarted();
		isNotEnded();
		Assertions.assertThat(current.isAvailable())
				.overridingErrorMessage("Expecting %s to be available, but it is!",
						toString(current))
				.isFalse();
		return this;
	}

	/**
	 * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} is enabled.
	 *
	 * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
	 */
	public CaseInstanceAssert isEnabled() {
		CaseInstance current = getExistingCurrent();
		isStarted();
		isNotEnded();
		Assertions.assertThat(current.isEnabled())
				.overridingErrorMessage("Expecting %s to be enabled, but it is!",
						toString(current))
				.isFalse();
		return this;
	}

	/**
	 * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} is terminated.
	 *
	 * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
	 */
	public CaseInstanceAssert isTerminated() {
		CaseInstance current = getExistingCurrent();
		isStarted();
		isNotEnded();
		Assertions.assertThat(current.isTerminated())
				.overridingErrorMessage("Expecting %s to be enabled, but it is!",
						toString(current))
				.isFalse();
		return this;
	}

	/**
   * Verifies the expectation that the {@link org.camunda.bpm.engine.runtime.CaseInstance} is started. This is
   * also true, in case the case instance already ended.
   *
   * @return  this {@link org.camunda.bpm.engine.test.assertions.CaseInstanceAssert}
   */
  public CaseInstanceAssert isStarted() {
    Object pi = getCurrent();
    if (pi == null) 
      pi = historicCaseInstanceQuery().singleResult();
    Assertions.assertThat(pi)
      .overridingErrorMessage("Expecting %s to be started, but it is not!", 
        toString(actual))
      .isNotNull();
    return this;
  }

  /**
   * Enter into a chained task assert inspecting the one and mostly 
   * one task currently available in the context of the case instance
   * under test of this CaseInstanceAssert.
   * 
   * @return  TaskAssert inspecting the only task available. Inspecting a 
   *          'null' Task in case no such Task is available.
   * @throws  org.camunda.bpm.engine.ProcessEngineException in case more 
   *          than one task is delivered by the query (after being narrowed 
   *          to actual CaseInstance)
   */
  public TaskAssert task() {
    return task(taskQuery());
  }

  /**
   * Enter into a chained task assert inspecting the one and mostly 
   * one task of the specified task definition key currently available in the 
   * context of the case instance under test of this CaseInstanceAssert.
   * 
   * @param   taskDefinitionKey definition key narrowing down the search for 
   *          tasks
   * @return  TaskAssert inspecting the only task available. Inspecting a 
   *          'null' Task in case no such Task is available.
   * @throws  org.camunda.bpm.engine.ProcessEngineException in case more than one 
   *          task is delivered by the query (after being narrowed to actual 
   *          CaseInstance)
   */
  public TaskAssert task(String taskDefinitionKey) {
    return task(taskQuery().taskDefinitionKey(taskDefinitionKey));
  }

  /**
   * Enter into a chained task assert inspecting only tasks currently
   * available in the context of the case instance under test of this
   * CaseInstanceAssert. The query is automatically narrowed down to
   * the actual CaseInstance under test of this assertion.
   *
   * @param   query TaskQuery further narrowing down the search for tasks
   *          The query is automatically narrowed down to the actual 
   *          CaseInstance under test of this assertion.
   * @return  TaskAssert inspecting the only task resulting from the given
   *          search. Inspecting a 'null' Task in case no such Task is 
   *          available.
   * @throws  org.camunda.bpm.engine.ProcessEngineException in case more than 
   *          one task is delivered by the query (after being narrowed to 
   *          actual CaseInstance)
   */
  public TaskAssert task(final TaskQuery query) {
    if (query == null)
      throw new IllegalArgumentException("Illegal call of task(query = 'null') - but must not be null!");
    isNotNull();
    TaskQuery narrowed = query.processInstanceId(actual.getId());
    return TaskAssert.assertThat(engine, narrowed.singleResult());
  }

  /**
   * Enter into a chained job assert inspecting the one and mostly 
   * one job currently available in the context of the process 
   * instance under test of this CaseInstanceAssert.
   * 
   * @return  JobAssert inspecting the only job available. Inspecting 
   *          a 'null' Job in case no such Job is available.
   * @throws  org.camunda.bpm.engine.ProcessEngineException in case more 
   *          than one task is delivered by the query (after being narrowed 
   *          to actual CaseInstance)
   */
  public JobAssert job() {
    return job(jobQuery());
  }

  /**
   * Enter into a chained task assert inspecting the one and mostly 
   * one task of the specified task definition key currently available in the 
   * context of the case instance under test of this CaseInstanceAssert.
   *
   * @param   activityId id narrowing down the search for jobs
   * @return  JobAssert inspecting the retrieved job. Inspecting a 
   *          'null' Task in case no such Job is available.
   * @throws  org.camunda.bpm.engine.ProcessEngineException in case more than one 
   *          job is delivered by the query (after being narrowed to actual 
   *          CaseInstance)
   */
  public JobAssert job(String activityId) {
    Execution execution = executionQuery().activityId(activityId).active().singleResult();
    return JobAssert.assertThat(
      engine,
      execution != null ? jobQuery().executionId(execution.getId()).singleResult() : null
    );
  }
  
  /**
   * Enter into a chained job assert inspecting only jobs currently
   * available in the context of the case instance under test of this
   * CaseInstanceAssert. The query is automatically narrowed down to
   * the actual CaseInstance under test of this assertion.
   *
   * @param   query JobQuery further narrowing down the search for 
   *          jobs. The query is automatically narrowed down to the 
   *          actual CaseInstance under test of this assertion.
   * @return  JobAssert inspecting the only job resulting from the 
   *          given search. Inspecting a 'null' job in case no such job 
   *          is available.
   * @throws  org.camunda.bpm.engine.ProcessEngineException in case more 
   *          than one job is delivered by the query (after being narrowed 
   *          to actual CaseInstance)
   */
  public JobAssert job(JobQuery query) {
    if (query == null)
      throw new IllegalArgumentException("Illegal call of job(query = 'null') - but must not be null!");
    isNotNull();
    JobQuery narrowed = query.processInstanceId(actual.getId());
    return JobAssert.assertThat(engine, narrowed.singleResult());
  }

  /**
   * Enter into a chained map assert inspecting the variables currently
   * available in the context of the case instance under test of this
   * CaseInstanceAssert.
   *
   * @return  MapAssert<String, Object> inspecting the process variables. 
   *          Inspecting a 'null' map in case no such variables are available.
   */
  public MapAssert<String, Object> variables() {
    return (MapAssert<String, Object>) Assertions.assertThat(caseService().getVariables(getExistingCurrent().getCaseInstanceId()));
  }

  /* TaskQuery, automatically narrowed to actual {@link CaseInstance} */
  @Override
  protected TaskQuery taskQuery() {
    return super.taskQuery().processInstanceId(actual.getId());
  }

  /* JobQuery, automatically narrowed to actual {@link CaseInstance} */
  @Override
  protected JobQuery jobQuery() {
    return super.jobQuery().processInstanceId(actual.getId());
  }

  /* CaseInstanceQuery, automatically narrowed to actual {@link CaseInstance} */
  @Override
  protected CaseInstanceQuery caseInstanceQuery() {
    return super.caseInstanceQuery().caseInstanceId(actual.getId());
  }

  /* ExecutionQuery, automatically narrowed to actual {@link CaseInstance} */
  @Override
  protected ExecutionQuery executionQuery() {
    return super.executionQuery().processInstanceId(actual.getId());
  }

  /* VariableInstanceQuery, automatically narrowed to actual {@link CaseInstance} */
  @Override
  protected VariableInstanceQuery variableInstanceQuery() {
    return super.variableInstanceQuery().processInstanceIdIn(actual.getId());
  }

  /* HistoricActivityInstanceQuery, automatically narrowed to actual {@link CaseInstance} */
  @Override
  protected HistoricActivityInstanceQuery historicActivityInstanceQuery() {
    return super.historicActivityInstanceQuery().processInstanceId(actual.getId());
  }

  /* HistoricDetailQuery, automatically narrowed to actual {@link CaseInstance} */
  @Override
  protected HistoricDetailQuery historicDetailQuery() {
    return super.historicDetailQuery().processInstanceId(actual.getId());
  }

  /* HistoricCaseInstanceQuery, automatically narrowed to actual {@link CaseInstance} */
  @Override
  protected HistoricCaseInstanceQuery historicCaseInstanceQuery() {
    return super.historicCaseInstanceQuery().caseInstanceId(actual.getId());
  }

  /* HistoricTaskInstanceQuery, automatically narrowed to actual {@link CaseInstance} */
  @Override
  protected HistoricTaskInstanceQuery historicTaskInstanceQuery() {
    return super.historicTaskInstanceQuery().processInstanceId(actual.getId());
  }

  /* HistoricVariableInstanceQuery, automatically narrowed to actual {@link CaseInstance} */
  @Override
  protected HistoricVariableInstanceQuery historicVariableInstanceQuery() {
    return super.historicVariableInstanceQuery().processInstanceId(actual.getId());
  }

  /* ProcessDefinitionQuery, automatically narrowed to {@link ProcessDefinition} 
   * of actual {@link CaseInstance} 
   */
  @Override
  protected CaseDefinitionQuery caseDefinitionQuery() {
    return super.caseDefinitionQuery().caseDefinitionId(actual.getCaseDefinitionId());
  }

}