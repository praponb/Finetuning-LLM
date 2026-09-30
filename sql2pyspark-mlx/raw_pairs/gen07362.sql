SELECT id, dept_name
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS latest_rank
    FROM departments
) t
WHERE latest_rank = 1
