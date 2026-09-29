SELECT
    id,
    dept_id,
    salary,
    SUM(salary) OVER (PARTITION BY dept_id) AS dept_id_sum_salary
FROM employees;
