WITH matched_keys AS (
    -- Set difference on keys
    SELECT location, dept_name
    FROM departments
    EXCEPT ALL
    SELECT location, dept_name
    FROM departments
    WHERE dept_name = 'HR'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    JOIN matched_keys k ON s.location = k.location AND s.dept_name = k.dept_name
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN budget >= 3511000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    dept_name,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names
FROM reconciled
GROUP BY CASE WHEN budget >= 3511000 THEN 'LARGE' ELSE 'SMALL' END, dept_name
HAVING SIZE(COLLECT_SET(dept_name)) < 1
