WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(full_name, 'unassigned') full_name_filled
    FROM employees
    WHERE job_title IS NOT NULL
      AND level IS DISTINCT FROM 'L5'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(salary) OVER w_running last7_max_salary,
        COUNT(salary) OVER w_running last7_count_salary,
        SUM(salary) OVER w_running last7_sum_salary,
        LEAD(salary, 1) OVER (PARTITION BY manager_id ORDER BY rank_in_group, id) next_salary
    FROM ranked
    WINDOW w_running AS (PARTITION BY dept_id ORDER BY rank_in_group, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC NULLS FIRST, id) AS rank_in_group_2
        FROM metrics
    ) ranked
    WHERE rank_in_group_2 = 1
)
-- Aggregate with conditional metrics
SELECT
    level,
    SUM(last7_count_salary) FILTER (WHERE job_title = 'Analyst') AS analyst_total_last7_count_salary,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list
FROM latest
GROUP BY level
ORDER BY analyst_total_last7_count_salary
