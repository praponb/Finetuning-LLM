SELECT
    id,
    dept_id,
    hire_date,
    salary,
    job_title,
    salary - LAG(salary, 1) OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS salary_change,
    RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC) AS rnk
FROM employees
WHERE full_name IS NULL
ORDER BY dept_id DESC, hire_date ASC
