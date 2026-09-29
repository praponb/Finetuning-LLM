WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN budget >= 3053000 THEN 'High' WHEN budget >= 735000 THEN 'Medium' ELSE 'Low' END AS budget_tier
    FROM departments
    WHERE location IS NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_tier
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) = 1
),
deduped_departments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        latest.salary
    FROM deduped_departments s
    JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_last
    EXCEPT DISTINCT
    SELECT id
    FROM departments
    WHERE location IN ('Tokyo', 'Singapore', 'London')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    JOIN kept_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    location,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    SUM(budget) AS total_budget
FROM kept_rows
GROUP BY 1
HAVING SUM(budget) < 1000000
ORDER BY location DESC;
