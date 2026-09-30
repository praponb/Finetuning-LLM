WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST, id) AS latest_rank
    FROM departments
    QUALIFY latest_rank = 1
)
SELECT
    id,
    dept_name,
    budget,
    latest_rank
FROM ranked
ORDER BY latest_rank NULLS LAST;
