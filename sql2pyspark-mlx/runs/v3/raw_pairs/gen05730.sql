SELECT
    id,
    dept_id,
    salary,
    MAX(salary) OVER (PARTITION BY dept_id) dept_id_max_salary
FROM employees;
