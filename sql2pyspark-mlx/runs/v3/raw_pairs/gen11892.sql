WITH ranked_employees AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS pos
    FROM employees
)
SELECT *
FROM ranked_employees
WHERE pos <= 5
