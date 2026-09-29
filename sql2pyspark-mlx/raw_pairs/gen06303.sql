WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(dept_name) AS dept_name_upper,
        CASE WHEN budget >= 4479000 THEN 'High' WHEN budget >= 1515000 THEN 'Medium' ELSE 'Low' END AS budget_level
    FROM departments
    WHERE location IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary <= 89000
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) = 1
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        top1.job_title
    FROM deduped s
    JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    dept_name,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(dept_name = 'Sales') AS sales_events,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM with_last
GROUP BY 1;
