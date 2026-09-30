WITH department_totals AS (
    SELECT dept_id, COUNT(CASE WHEN level = 'L3' THEN 1 END) l3_count
    FROM employees
    GROUP BY dept_id
)
SELECT d.budget, d.dept_name, dt.l3_count
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.l3_count >= 7
ORDER BY dt.l3_count
