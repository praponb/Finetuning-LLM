WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(location, 'unassigned') AS location_filled,
        COALESCE(location, 'unassigned') AS location_filled_2,
        COALESCE(location, 'OTHER') AS resolved_location
    FROM departments
    WHERE dept_name IN ('Engineering', 'Sales', 'Marketing')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT location
    FROM valid
    INTERSECT ALL
    SELECT location
    FROM departments
    WHERE dept_name NOT IN ('Finance', 'HR')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN kept_keys k ON s.location = k.location
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS budget_percent_rank,
        MAX(budget) OVER w_recent AS location_max_budget,
        AVG(budget) OVER w_recent AS location_avg_budget,
        COUNT(budget) OVER w_part AS dept_name_count_budget,
        STDDEV_SAMP(budget) OVER w_part_2 AS location_stddev_samp_budget
    FROM reconciled
    WINDOW w_recent AS (PARTITION BY location), w_part AS (PARTITION BY dept_name), w_part_2 AS (PARTITION BY location, dept_name)
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id IN (4, 5, 37)
        )
)
-- Grouped metrics
SELECT
    location,
    CASE WHEN budget_percent_rank >= 0.77 THEN 'HIGH' ELSE 'LOW' END AS budget_percent_rank_tier,
    PERCENTILE_APPROX(location_stddev_samp_budget, ARRAY(0.25, 0.5, 0.75)) AS location_stddev_samp_budget_quartiles,
    SUM(CASE WHEN dept_name = 'Marketing' THEN location_max_budget ELSE 0 END) AS marketing_location_max_budget,
    APPROX_PERCENTILE(dept_name_count_budget, 0.9) AS dept_name_count_budget_p90,
    ROUND(AVG(location_stddev_samp_budget), 2) AS avg_location_stddev_samp_budget,
    AVG(location_stddev_samp_budget) AS avg_location_stddev_samp_budget_2,
    PERCENTILE_APPROX(dept_name_count_budget, ARRAY(0.25, 0.5, 0.75)) AS dept_name_count_budget_quartiles
FROM eligible
GROUP BY location, CASE WHEN budget_percent_rank >= 0.77 THEN 'HIGH' ELSE 'LOW' END
HAVING SUM(CASE WHEN dept_name = 'Marketing' THEN location_max_budget ELSE 0 END) > 0
ORDER BY avg_location_stddev_samp_budget DESC
