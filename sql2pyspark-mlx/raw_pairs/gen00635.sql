SELECT
    id,
    full_name,
    email,
    dept_id,
    salary,
    hire_date,
    level,
    COUNT(salary) OVER (PARTITION BY manager_id) AS manager_id_count_salary,
    MIN(salary) OVER (PARTITION BY DATE_TRUNC('WEEK', hire_date), dept_id) AS hire_date_period_min_salary
FROM employees
WHERE dept_id IN (7, 27)
