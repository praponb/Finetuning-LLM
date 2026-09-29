SELECT
    id,
    dept_name,
    location,
    budget
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS dedup_rank
    FROM departments
) ranked
WHERE dedup_rank = 1
