SELECT
    id,
    full_name,
    manager_id,
    job_title,
    salary,
    level,
    CUME_DIST() OVER (ORDER BY salary) AS salary_cume_dist,
    AVG(salary) OVER (PARTITION BY manager_id, dept_id) AS manager_id_avg_salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC, id) AS event_seq,
    MAX(salary) OVER (PARTITION BY DATE_TRUNC('week', hire_date), manager_id) AS hire_date_period_max_salary
FROM employees
