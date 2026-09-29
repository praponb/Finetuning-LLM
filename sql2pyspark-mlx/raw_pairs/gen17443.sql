WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.level IN ('L2', 'L3')
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS LAST, id) AS latest_rank
        FROM not_flagged
    ) x
    WHERE latest_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget,
        latest_rank,
        FIRST_VALUE(location, TRUE) OVER (PARTITION BY location ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_location,
        MAX(budget) OVER (PARTITION BY dept_name, location) dept_name_max_budget,
        LAST_VALUE(dept_name, TRUE) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_dept_name,
        RANK() OVER (ORDER BY budget NULLS FIRST) budget_rank
    FROM ranked
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(dept_name_max_budget, 2, 0) OVER (PARTITION BY dept_name ORDER BY dept_name_max_budget, id) next_dept_name_max_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_rank, id) seq_no
    FROM metrics
    WHERE latest_rank <> 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT dept_name, id
    FROM scored
    EXCEPT DISTINCT
    SELECT dept_name, id
    FROM departments
    WHERE budget >= 1814000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.dept_name = k.dept_name AND s.id = k.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title IN ('Analyst', 'Manager')
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.salary >= 136000
        )
)
-- Grouped metrics
SELECT
    CASE WHEN budget >= 4059000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    COUNT_IF(dept_name = 'Sales') AS sales_events,
    COUNT_IF(dept_name = 'Sales') AS sales_events_2,
    COUNT(DISTINCT dept_name) AS distinct_dept_names
FROM kept
GROUP BY CASE WHEN budget >= 4059000 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END;
