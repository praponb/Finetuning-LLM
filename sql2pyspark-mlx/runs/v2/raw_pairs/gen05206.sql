SELECT
    id,
    salary,
    hire_date,
    ROUND(salary, 1) AS salary_rounded
FROM employees
WHERE dept_id IN (12, 28, 34);
