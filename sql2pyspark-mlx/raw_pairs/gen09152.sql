WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        TRIM(job_title) clean_job_title,
        COALESCE(level, 'NONE') resolved_level
    FROM employees
    WHERE job_title IN ('Engineer', 'Intern', 'Manager')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(salary, 1) OVER (PARTITION BY manager_id ORDER BY hire_date, id) next_salary,
        FIRST_VALUE(level) IGNORE NULLS OVER w_part first_known_level,
        AVG(salary) OVER (ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) salary_avg_2
    FROM base
    WHERE email LIKE '%@GMAIL.COM'
    WINDOW w_part AS (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(salary) OVER w_grp running_min_salary,
        MAX(salary) OVER w_grp running_max_salary,
        SUM(salary) OVER w_grp running_sum_salary,
        LAG(salary_avg_2, 1) OVER (PARTITION BY manager_id ORDER BY next_salary, id) prev_salary_avg_2
    FROM scored
    WINDOW w_grp AS (PARTITION BY level, manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        salary,
        is_remote,
        level,
        clean_job_title,
        resolved_level,
        running_max_salary,
        prev_salary_avg_2,
        AVG(running_max_salary) OVER (ORDER BY prev_salary_avg_2, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) running_max_salary_avg_2_prior,
        AVG(running_sum_salary) OVER w_ordered running_sum_salary_avg_5_prior,
        COUNT(salary) OVER w_part manager_id_count_salary
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY level ORDER BY salary_avg_2, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_part AS (PARTITION BY manager_id, level)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, level
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id, level
    FROM employees
    WHERE manager_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id AND s.level = k.level
),
base_employees AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        salary,
        is_remote,
        level,
        clean_job_title,
        resolved_level,
        running_sum_salary_avg_5_prior,
        manager_id_count_salary,
        CASE WHEN level IN ('L1', 'L4') THEN 'Core' ELSE 'Other' END level_group
    FROM kept_rows
    WHERE level IS NOT NULL
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(running_sum_salary_avg_5_prior) OVER (PARTITION BY level) level_count_running_sum_salary_avg_5_prior,
        ROUND(manager_id_count_salary / SUM(manager_id_count_salary) OVER (PARTITION BY level), 4) manager_id_count_salary_share
    FROM base_employees
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN manager_id_count_salary >= 15 THEN 'LARGE' ELSE 'SMALL' END AS manager_id_count_salary_tier,
    CASE WHEN level_count_running_sum_salary_avg_5_prior >= 15 THEN 'HIGH' ELSE 'LOW' END AS level_count_running_sum_salary_avg_5_prior_tier,
    level,
    ROUND(AVG(manager_id_count_salary), 2) AS avg_manager_id_count_salary,
    AVG(manager_id_count_salary_share) AS avg_manager_id_count_salary_share,
    PERCENTILE_APPROX(salary, 0.95) AS salary_p95,
    STDDEV_SAMP(salary) AS salary_stddev,
    SUM(running_sum_salary_avg_5_prior) AS total_running_sum_salary_avg_5_prior,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN manager_id_count_salary >= 15 THEN 'LARGE' ELSE 'SMALL' END ORDER BY AVG(manager_id_count_salary_share) DESC) AS avg_manager_id_count_salary_share_dense_rank
FROM scored_employees
GROUP BY 1, 2, 3
ORDER BY level
