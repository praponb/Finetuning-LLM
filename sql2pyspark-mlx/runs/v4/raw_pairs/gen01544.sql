WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(location, ' - ', dept_name) AS location_dept_name,
        COALESCE(dept_name, 'N/A') AS resolved_dept_name
    FROM departments
    WHERE location IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(budget, 2) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget,
        SUM(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        MIN(budget) OVER w_part AS dept_name_min_budget,
        LAG(budget, 1, 0) OVER (PARTITION BY dept_name ORDER BY budget, id) AS prev_budget_2,
        DENSE_RANK() OVER w_ordered AS budget_dense_rank
    FROM filtered
    WINDOW w_part AS (PARTITION BY dept_name), w_ordered AS (PARTITION BY location ORDER BY budget NULLS LAST)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.dept_id IN (2, 4, 21)
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_budget >= 3242000 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_budget_tier,
    COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END) AS marketing_count,
    COUNT(*) FILTER (WHERE dept_name = 'HR') AS hr_rows
FROM eligible
GROUP BY CASE WHEN prev_budget >= 3242000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING marketing_count > 1
