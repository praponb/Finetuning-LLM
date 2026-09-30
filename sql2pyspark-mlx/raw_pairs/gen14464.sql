SELECT
    id,
    full_name,
    email,
    manager_id,
    job_title,
    is_remote,
    level,
    AVG(salary) OVER (ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) salary_avg_2_prior,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', hire_date), job_title ORDER BY hire_date, id) row_seq,
    LEAD(salary, 1) OVER (ORDER BY salary, id) next_salary
FROM employees
