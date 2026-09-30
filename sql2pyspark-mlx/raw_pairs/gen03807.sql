WITH cleaned AS (
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        CASE WHEN level IN ('L3', 'L1') THEN 'Group A' ELSE 'Other' END AS level_group
    FROM employees
    WHERE hire_date > DATE '2023-05-10'
      AND job_title IS NOT NULL
),
metrics AS (
    SELECT
        *,
        SUM(salary) OVER (PARTITION BY dept_id, manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        SUM(salary) OVER w_hist AS running_salary_2,
        LEAD(salary, 2) OVER (PARTITION BY DATE_TRUNC('WEEK', hire_date), manager_id ORDER BY salary, id) AS next_salary
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY dept_id ORDER BY salary, id), w_hist AS (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        id,
        email,
        hire_date,
        is_remote,
        level,
        level_group,
        running_salary,
        seq_no,
        next_salary,
        LAST_VALUE(next_salary, TRUE) OVER (PARTITION BY manager_id, level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_salary,
        RANK() OVER (ORDER BY seq_no NULLS LAST) AS seq_no_rank
    FROM metrics
),
base AS (
    SELECT
        *,
        DATE_FORMAT(hire_date, 'yyyy-MM-dd') AS hire_date_label,
        running_salary * last_known_next_salary AS running_salary_times_last_known_next_salary
    FROM with_windows
    WHERE hire_date > DATE '2023-06-09'
      AND level IS NOT NULL
),
with_windows_employees AS (
    SELECT
        id,
        email,
        hire_date,
        is_remote,
        level,
        seq_no,
        next_salary,
        seq_no_rank,
        hire_date_label,
        running_salary_times_last_known_next_salary,
        AVG(next_salary) OVER (PARTITION BY level ORDER BY seq_no, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next_salary_avg_2,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY seq_no_rank, id) AS event_seq,
        NTILE(5) OVER (ORDER BY seq_no_rank, id) AS seq_no_rank_bucket
    FROM base
    WHERE hire_date <= '2025-01-01'
)
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    level,
    MAX(event_seq) AS max_event_seq,
    SUM(seq_no_rank) FILTER (WHERE level = 'L1') AS l1_total_seq_no_rank,
    COUNT(*) AS row_count
FROM with_windows_employees
GROUP BY DATE_TRUNC('YEAR', hire_date), DATE_TRUNC('MONTH', hire_date), level
ORDER BY l1_total_seq_no_rank NULLS FIRST;
