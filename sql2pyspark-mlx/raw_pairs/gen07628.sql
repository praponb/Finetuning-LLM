WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS(',', dept_name, location) dept_name_location_key,
        INITCAP(TRIM(location)) location_title
    FROM departments
    WHERE dept_name IS NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT dept_name
    FROM latest
    INTERSECT ALL
    SELECT dept_name
    FROM departments
    WHERE location IN ('Bangkok', 'New York', 'Chiang Mai')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN kept_keys k ON s.dept_name = k.dept_name
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) budget_share,
        RANK() OVER (PARTITION BY dept_name, location ORDER BY budget DESC NULLS LAST) budget_rank,
        AVG(budget) OVER (PARTITION BY dept_name) dept_name_avg_budget
    FROM reconciled
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.is_remote
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        location,
        dept_name,
        budget
    FROM not_flagged
) src
PIVOT (
    SUM(budget)
    FOR dept_name IN ('Marketing', 'Legal')
);
