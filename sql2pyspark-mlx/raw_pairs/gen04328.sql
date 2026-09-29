WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(salary AS INT) AS salary_int
    FROM employees
    WHERE level IS NOT NULL
      AND job_title IS DISTINCT FROM 'Designer'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(salary) OVER w_recent AS running_count_salary,
        AVG(salary) OVER w_recent AS running_avg_salary,
        SUM(salary) OVER w_recent AS running_sum_salary,
        AVG(salary_int) OVER (PARTITION BY dept_id, job_title ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_int_avg_3_prior
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY dept_id ORDER BY salary_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        manager_id,
        job_title,
        salary,
        hire_date,
        salary_int,
        running_count_salary,
        running_avg_salary,
        running_sum_salary,
        LEAD(running_count_salary, 1) OVER (PARTITION BY MONTH(hire_date), dept_id ORDER BY salary_int_avg_3_prior, id) AS next_running_count_salary,
        LAST_VALUE(salary_int) IGNORE NULLS OVER w_part AS last_known_salary_int
    FROM metrics
    WHERE hire_date > '2024-05-07'
    WINDOW w_part AS (PARTITION BY MONTH(hire_date), job_title ORDER BY salary_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(last_known_salary_int) OVER w_hist AS running_count_last_known_salary_int,
        AVG(last_known_salary_int) OVER w_hist AS running_avg_last_known_salary_int,
        SUM(last_known_salary_int) OVER w_hist AS running_sum_last_known_salary_int
    FROM scored
    WINDOW w_hist AS (PARTITION BY manager_id ORDER BY running_count_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_employees AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(next_running_count_salary) OVER (PARTITION BY manager_id ORDER BY running_sum_last_known_salary_int, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS next_running_count_salary_avg_6,
        SUM(salary) OVER (PARTITION BY manager_id, job_title) AS manager_id_sum_salary,
        NTILE(4) OVER w_recent AS running_sum_last_known_salary_int_bucket
    FROM scored_employees
    WINDOW w_recent AS (PARTITION BY manager_id ORDER BY running_sum_last_known_salary_int DESC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        job_title,
        running_avg_last_known_salary_int,
        running_avg_salary
    FROM metrics_employees
) src
UNPIVOT (
    value FOR measure IN (running_avg_last_known_salary_int AS RUNNING_AVG_LAST_KNOWN_SALARY_INT_VAL, running_avg_salary AS RUNNING_AVG_SALARY)
);
