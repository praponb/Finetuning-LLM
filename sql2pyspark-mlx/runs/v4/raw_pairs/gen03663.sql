WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        LAG(salary, 1) OVER (PARTITION BY job_title ORDER BY salary, id) AS prev_salary,
        FIRST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title
    FROM employees
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(prev_salary) OVER (PARTITION BY manager_id ORDER BY running_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_salary,
        ROUND(prev_salary / SUM(prev_salary) OVER (PARTITION BY level), 4) AS prev_salary_share,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY running_salary DESC, id) AS event_seq,
        RANK() OVER (PARTITION BY dept_id ORDER BY running_salary) AS running_salary_rank
    FROM metrics
),
with_windows_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(prev_salary, 1) OVER (ORDER BY event_seq, id) AS prev_prev_salary,
        MIN(running_salary_rank) OVER (PARTITION BY job_title) AS job_title_min_running_salary_rank,
        FIRST_VALUE(cumulative_prev_salary, TRUE) OVER w_part AS first_known_cumulative_prev_salary,
        DENSE_RANK() OVER w_hist AS cumulative_prev_salary_dense_rank
    FROM with_windows
    WINDOW w_part AS (PARTITION BY manager_id ORDER BY prev_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY job_title ORDER BY cumulative_prev_salary NULLS LAST)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(running_salary) OVER (PARTITION BY job_title) AS job_title_avg_running_salary,
        AVG(cumulative_prev_salary) OVER (PARTITION BY manager_id ORDER BY first_known_cumulative_prev_salary, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cumulative_prev_salary_avg_5,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY cumulative_prev_salary, id) AS row_seq,
        DENSE_RANK() OVER (PARTITION BY level, manager_id ORDER BY cumulative_prev_salary NULLS FIRST) AS cumulative_prev_salary_dense_rank_2,
        LAST_VALUE(level, TRUE) OVER (PARTITION BY job_title ORDER BY prev_salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level
    FROM with_windows_employees
)
SELECT
    s.id,
    s.email,
    s.dept_id,
    s.manager_id,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    s.running_salary,
    CASE WHEN level RLIKE '^(L4|L3)$' THEN 'CORE' WHEN level RLIKE '^(L1|L2)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS level_zone,
    (SELECT MAX(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS max_employee_salary
FROM enriched s
