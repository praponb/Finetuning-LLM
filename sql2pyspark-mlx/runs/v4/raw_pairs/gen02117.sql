WITH department_totals AS (
    SELECT dept_id, ROUND((100 * SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l4
    FROM employees
    WHERE MONTH(hire_date) IN (1, 7, 12)
    GROUP BY dept_id
)
SELECT d.location, dt.pct_l4
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.pct_l4 > 45
ORDER BY dt.pct_l4 DESC, d.location DESC;
