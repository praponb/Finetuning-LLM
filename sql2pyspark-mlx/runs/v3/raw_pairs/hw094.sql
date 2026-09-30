WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rn
    FROM employees
)
SELECT * FROM ranked WHERE rn = 1;
