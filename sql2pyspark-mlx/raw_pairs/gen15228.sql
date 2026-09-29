WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.salary <= 102000
        )
)
-- Grouped metrics
SELECT
    CASE WHEN budget >= 3592000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    SIZE(COLLECT_SET(location)) AS num_locations,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    STDDEV_SAMP(budget) AS budget_stddev,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles_2,
    SIZE(COLLECT_SET(location)) AS num_locations_2
FROM kept
GROUP BY CASE WHEN budget >= 3592000 THEN 'HIGH' ELSE 'LOW' END
HAVING STDDEV_SAMP(budget) >= 0
ORDER BY budget_stddev DESC
