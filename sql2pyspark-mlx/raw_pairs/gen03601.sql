WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(budget) OVER w_part AS last4_max_budget,
        COUNT(budget) OVER w_part AS last4_count_budget,
        AVG(budget) OVER w_part AS last4_avg_budget,
        SUM(budget) OVER (PARTITION BY location, dept_name) AS location_sum_budget
    FROM departments
    WHERE location IN ('Singapore', 'New York')
    WINDOW w_part AS (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT dept_name, id
    FROM enriched
    EXCEPT DISTINCT
    SELECT dept_name, id
    FROM departments
    WHERE dept_name NOT IN ('Engineering', 'Sales', 'Marketing')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.dept_name = k.dept_name AND s.id = k.id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level != 'L1'
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id IN (3, 21, 23, 25)
        )
),
scored AS (
    -- Ranking and gap features
    SELECT *, SUM(location_sum_budget) OVER (PARTITION BY location, dept_name) AS location_sum_location_sum_budget
    FROM not_flagged
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.email IS NOT NULL
        )
)
SELECT
    id,
    dept_name,
    location,
    budget,
    last4_count_budget,
    location_sum_budget,
    CASE WHEN dept_name RLIKE '^(Engineering|Marketing)$' THEN 'OTHER_ZONE' WHEN dept_name RLIKE '^(Sales|HR)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS dept_name_zone
FROM kept
ORDER BY budget DESC NULLS LAST, location_sum_budget DESC
