WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_name,
        location,
        budget
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST, id) AS dedup_rank
        FROM departments
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    id,
    dept_name,
    location,
    budget
FROM latest
ORDER BY budget NULLS LAST, location DESC;
