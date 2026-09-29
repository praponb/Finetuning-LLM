WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        MIN(salary) OVER w_recent AS running_min_salary,
        COUNT(salary) OVER w_recent AS running_count_salary,
        AVG(salary) OVER w_recent AS running_avg_salary,
        SUM(salary) OVER w_recent AS running_sum_salary,
        AVG(salary) OVER w_part AS salary_avg_6
    FROM employees
    WHERE level = 'L3'
    WINDOW w_recent AS (PARTITION BY job_title, level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
metrics_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(running_sum_salary, TRUE) OVER w_grp AS last_known_running_sum_salary,
        FIRST_VALUE(running_avg_salary) IGNORE NULLS OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_salary
    FROM metrics
    WINDOW w_grp AS (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY MONTH(hire_date), job_title ORDER BY running_avg_salary) AS running_avg_salary_dense_rank,
        LEAD(salary, 1) OVER (PARTITION BY job_title ORDER BY running_sum_salary, id) AS next_salary
    FROM metrics_employees
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        salary,
        hire_date,
        level,
        running_min_salary,
        running_count_salary,
        running_avg_salary,
        running_sum_salary,
        last_known_running_sum_salary,
        running_avg_salary_dense_rank,
        ROUND(running_avg_salary_dense_rank * 0.9, 2) AS adjusted_running_avg_salary_dense_rank,
        TRIM(email) AS clean_email
    FROM with_windows
    WHERE hire_date > TO_DATE('2023-03-14')
      AND job_title IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        hire_date,
        running_min_salary,
        running_count_salary,
        running_avg_salary,
        running_sum_salary,
        running_avg_salary_dense_rank,
        adjusted_running_avg_salary_dense_rank,
        clean_email,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('week', hire_date) ORDER BY salary DESC, id) AS seq_no,
        MAX(running_avg_salary_dense_rank) OVER (PARTITION BY level) AS level_max_running_avg_salary_dense_rank,
        LEAD(last_known_running_sum_salary, 2, 0) OVER (PARTITION BY level ORDER BY running_sum_salary, id) AS next_last_known_running_sum_salary,
        AVG(running_avg_salary) OVER (PARTITION BY level ORDER BY running_min_salary, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_avg_salary_avg_6_prior,
        AVG(running_min_salary) OVER (PARTITION BY level ORDER BY last_known_running_sum_salary, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_min_salary_avg_6_prior
    FROM cleaned
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    CASE WHEN level_max_running_avg_salary_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS level_max_running_avg_salary_dense_rank_tier,
    CASE WHEN seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(adjusted_running_avg_salary_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS adjusted_running_avg_salary_dense_rank_quartiles,
    ROUND(AVG(running_avg_salary_avg_6_prior), 2) AS avg_running_avg_salary_avg_6_prior,
    GROUPING(DATE_TRUNC('QUARTER', hire_date)) AS grouping_level
FROM scored
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', hire_date), CASE WHEN level_max_running_avg_salary_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('QUARTER', hire_date), CASE WHEN level_max_running_avg_salary_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('QUARTER', hire_date)));
