WITH department_totals AS (
    SELECT dept_id, MIN(hire_date) AS first_hire_date, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM employees
    WHERE full_name IS NOT NULL
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    dt.first_hire_date,
    dt.stddev_salary
FROM departments d
LEFT JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.stddev_salary >= 70
ORDER BY id DESC;
