WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        ADD_MONTHS(hire_date, 6) AS hire_date_plus_months,
        COALESCE(level, 'UNKNOWN') AS resolved_level
    FROM employees
    WHERE hire_date > DATE '2023-07-09'
      AND job_title IS DISTINCT FROM 'Analyst'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(salary) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        AVG(salary) OVER (PARTITION BY level ORDER BY hire_date_plus_months, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS salary_avg_3
    FROM valid
    WHERE hire_date_plus_months <= '2024-01-20'
    WINDOW w_ordered AS (PARTITION BY MONTH(hire_date) ORDER BY hire_date, id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(salary_avg_3) OVER w_part AS next_salary_avg_3,
        LAG(salary_avg_3) OVER w_part AS prev_salary_avg_3,
        LAG(salary) OVER w_part AS prev_salary,
        LEAD(salary) OVER w_part AS next_salary,
        ROW_NUMBER() OVER w_part AS seq_no,
        MIN(running_salary) OVER (PARTITION BY job_title, dept_id) AS job_title_min_running_salary
    FROM metrics
    WINDOW w_part AS (PARTITION BY manager_id ORDER BY salary, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(running_salary) OVER (PARTITION BY manager_id ORDER BY salary_avg_3, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_running_salary,
        MIN(running_salary) OVER (PARTITION BY manager_id ORDER BY salary_avg_3, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_running_salary,
        COUNT(running_salary) OVER (PARTITION BY manager_id ORDER BY salary_avg_3, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_running_salary,
        SUM(running_salary) OVER (PARTITION BY manager_id ORDER BY salary_avg_3, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_running_salary,
        AVG(next_salary) OVER (PARTITION BY level ORDER BY prev_salary, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_salary_avg_5_prior
    FROM scored
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        salary,
        hire_date,
        is_remote,
        level,
        resolved_level,
        running_salary,
        row_seq,
        salary_avg_3,
        next_salary_avg_3,
        next_salary,
        seq_no,
        last7_avg_running_salary,
        last7_min_running_salary,
        next_salary_avg_5_prior,
        MIN(prev_salary) OVER w_running AS running_min_prev_salary,
        SUM(prev_salary) OVER w_running AS running_sum_prev_salary,
        AVG(prev_salary) OVER w_running AS running_avg_prev_salary
    FROM with_windows
    WINDOW w_running AS (PARTITION BY job_title, manager_id ORDER BY last7_count_running_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('MONTH', hire_date) AS activity_month,
        CASE WHEN salary_avg_3 >= 144198 THEN 'HIGH' ELSE 'LOW' END AS salary_avg_3_tier,
        ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN level = 'L4' THEN 1 END), 0), 2) AS seq_no_per_l4,
        COUNT(CASE WHEN level = 'L3' THEN 1 END) AS l3_count,
        APPROX_PERCENTILE(last7_avg_running_salary, ARRAY(0.25, 0.5, 0.75)) AS last7_avg_running_salary_quartiles,
        PERCENTILE_APPROX(running_avg_prev_salary, 0.95) AS running_avg_prev_salary_p95
    FROM scored_employees
    GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', hire_date), CASE WHEN salary_avg_3 >= 144198 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('MONTH', hire_date)))
    HAVING PERCENTILE_APPROX(running_avg_prev_salary, 0.95) < 1000000
)
SELECT
    activity_month,
    salary_avg_3_tier,
    seq_no_per_l4,
    l3_count,
    last7_avg_running_salary_quartiles,
    running_avg_prev_salary_p95
FROM stats
ORDER BY l3_count DESC;
