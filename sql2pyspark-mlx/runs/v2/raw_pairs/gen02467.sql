WITH ranked_departments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_num
    FROM departments
    WHERE location NOT IN ('Tokyo', 'Berlin')
)
SELECT *
FROM ranked_departments
WHERE row_num <= 2
ORDER BY id DESC;
