SELECT
    id,
    hire_date,
    level,
    salary,
    job_title,
    dept_id,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('WEEK', hire_date), level ORDER BY salary DESC, id) AS row_num,
    LAG(salary, 1) OVER (PARTITION BY job_title, dept_id ORDER BY hire_date, id) AS prev_salary
FROM employees;
