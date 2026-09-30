WITH department_totals AS (
    SELECT dept_id, AVG(salary) AS avg_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values
    FROM employees
    WHERE YEAR(hire_date) >= 2024
    GROUP BY dept_id
)
SELECT
    d.budget,
    d.location,
    dt.avg_salary,
    dt.job_title_values
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id
ORDER BY dt.job_title_values, d.budget DESC;
