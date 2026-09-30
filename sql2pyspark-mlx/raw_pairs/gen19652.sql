WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        is_remote,
        level,
        SUBSTRING(full_name, 1, 4) AS full_name_prefix
    FROM employees
    WHERE hire_date > DATE '2023-09-06'
      AND level IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        salary,
        is_remote,
        MIN(salary) OVER w_ordered AS running_min_salary,
        AVG(salary) OVER w_ordered AS running_avg_salary,
        MAX(salary) OVER w_ordered AS running_max_salary,
        COUNT(salary) OVER (PARTITION BY level) AS level_count_salary
    FROM filtered
    WHERE LOWER(TRIM(full_name)) = 'jack taylor'
    WINDOW w_ordered AS (PARTITION BY level ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        salary,
        is_remote,
        running_min_salary,
        running_avg_salary,
        running_max_salary,
        level_count_salary,
        AVG(running_min_salary) OVER w_grp AS running_min_salary_avg_2,
        NTILE(10) OVER w_ordered AS running_max_salary_bucket
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY dept_id ORDER BY salary ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY dept_id ORDER BY running_max_salary DESC, id ASC)
),
with_windows_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        is_remote,
        AVG(level_count_salary) OVER w_hist AS running_avg_level_count_salary,
        MIN(level_count_salary) OVER w_hist AS running_min_level_count_salary,
        SUM(level_count_salary) OVER w_hist AS running_sum_level_count_salary
    FROM enriched
    WINDOW w_hist AS (PARTITION BY dept_id ORDER BY running_min_salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY running_sum_level_count_salary DESC, id ASC) AS row_num
    FROM with_windows_employees
    QUALIFY row_num = 1
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        s.*,
        b.budget,
        b.dept_name,
        b.location
    FROM ranked s
    JOIN departments b
        ON s.dept_id = b.id
)
-- Grouped metrics
SELECT
    CASE WHEN running_min_level_count_salary >= 16.8 THEN 'LARGE' ELSE 'SMALL' END AS running_min_level_count_salary_tier,
    CASE WHEN budget >= 3791000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    ROUND(SUM(running_sum_level_count_salary) / NULLIF(COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END), 0), 2) AS running_sum_level_count_salary_per_engineering,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    PERCENTILE_APPROX(running_min_level_count_salary, ARRAY(0.25, 0.5, 0.75)) AS running_min_level_count_salary_quartiles,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN running_min_level_count_salary >= 16.8 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(SUM(running_sum_level_count_salary) / NULLIF(COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END), 0), 2) DESC) AS running_sum_level_count_salary_per_engineering_dense_rank
FROM with_dims
GROUP BY CASE WHEN running_min_level_count_salary >= 16.8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN budget >= 3791000 THEN 'LARGE' ELSE 'SMALL' END;
