SELECT
    id,
    dept_id,
    hire_date,
    salary,
    job_title,
    SUM(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS running_salary,
    ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) AS row_num
FROM employees;
