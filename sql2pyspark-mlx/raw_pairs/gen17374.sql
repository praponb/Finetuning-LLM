WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS('-', location, dept_name) location_dept_name_key
    FROM departments
    WHERE budget > 2429000
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_name,
        budget,
        location_dept_name_key
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS dedup_rank
        FROM filtered
    ) x
    WHERE dedup_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(budget, 0) budget_filled
    FROM deduped
    WHERE dept_name IS NOT DISTINCT FROM 'Finance'
      AND dept_name IN ('Legal', 'Engineering')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget,
        location_dept_name_key,
        SUM(budget_filled) OVER w_hist running_sum_budget_filled,
        MIN(budget_filled) OVER w_hist running_min_budget_filled,
        MAX(budget_filled) OVER w_hist running_max_budget_filled,
        COUNT(budget_filled) OVER w_hist running_count_budget_filled
    FROM base
    WINDOW w_hist AS (PARTITION BY dept_name ORDER BY budget_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_departments AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        budget,
        location_dept_name_key,
        running_min_budget_filled,
        running_count_budget_filled,
        SUM(running_count_budget_filled) OVER w_part last7_sum_running_count_budget_filled,
        COUNT(running_count_budget_filled) OVER w_part last7_count_running_count_budget_filled,
        AVG(running_count_budget_filled) OVER w_part last7_avg_running_count_budget_filled,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) budget_share
    FROM with_windows
    WINDOW w_part AS (PARTITION BY dept_name ORDER BY running_max_budget_filled, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_min_budget_filled >= 66 THEN 'LARGE' ELSE 'SMALL' END AS running_min_budget_filled_tier,
    COUNT(*) AS row_count,
    AVG(last7_count_running_count_budget_filled) AS avg_last7_count_running_count_budget_filled,
    ROUND(AVG(last7_sum_running_count_budget_filled), 2) AS avg_last7_sum_running_count_budget_filled
FROM with_windows_departments
GROUP BY 1
ORDER BY row_count NULLS FIRST
