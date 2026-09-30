WITH base AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(dept_name)) AS dept_name_title
    FROM departments
    WHERE budget >= 3821000
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.level,
        latest.salary
    FROM base s
    JOIN LATERAL (
            SELECT c.level, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY salary DESC) AS rn
        FROM with_last
    ) x
    WHERE rn = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.level = 'L2'
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY salary NULLS LAST, id) AS dedup_rank
    FROM kept
    QUALIFY dedup_rank = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        level,
        budget
    FROM latest
) src
PIVOT (
    AVG(budget)
    FOR level IN ('L3' AS l3, 'L2' AS l2)
);
