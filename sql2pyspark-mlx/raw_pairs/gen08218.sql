WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        TRIM(email) AS clean_email
    FROM employees
    WHERE manager_id IS NOT NULL
      AND LENGTH(full_name) >= 9
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        salary,
        hire_date,
        is_remote,
        level,
        clean_email,
        AVG(salary) OVER w_running AS last7_avg_salary,
        MIN(salary) OVER w_running AS last7_min_salary,
        SUM(salary) OVER w_running AS last7_sum_salary,
        MAX(salary) OVER w_running AS last7_max_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title, level), 4) AS salary_share
    FROM cleaned
    WINDOW w_running AS (PARTITION BY manager_id, level ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(last7_avg_salary) OVER (PARTITION BY level ORDER BY salary, id) AS next_last7_avg_salary,
        LAG(last7_avg_salary) OVER (PARTITION BY level ORDER BY salary, id) AS prev_last7_avg_salary,
        LEAD(last7_max_salary) OVER (PARTITION BY level ORDER BY salary, id) AS next_last7_max_salary,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS event_seq
    FROM with_windows
),
with_windows_employees AS (
    -- Running and moving aggregates
    SELECT
        id,
        hire_date,
        is_remote,
        clean_email,
        last7_avg_salary,
        last7_sum_salary,
        next_last7_avg_salary,
        prev_last7_avg_salary,
        event_seq,
        LAST_VALUE(last7_max_salary) IGNORE NULLS OVER w_hist AS last_known_last7_max_salary,
        DENSE_RANK() OVER w_running AS last7_sum_salary_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY last7_sum_salary, id) AS row_seq
    FROM enriched
    WHERE next_last7_avg_salary < 78000
    WINDOW w_hist AS (PARTITION BY level ORDER BY next_last7_avg_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY YEAR(hire_date) ORDER BY last7_sum_salary DESC NULLS LAST)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    metric,
    metric_value
FROM (
    SELECT
        id,
        next_last7_avg_salary,
        last7_sum_salary
    FROM with_windows_employees
) src
UNPIVOT (
    metric_value FOR metric IN (next_last7_avg_salary AS NEXT_LAST7_AVG_SALARY_VAL, last7_sum_salary)
) u;
