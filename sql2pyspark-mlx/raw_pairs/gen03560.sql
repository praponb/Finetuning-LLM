SELECT
    *,
    LEAD(salary, 2) OVER (PARTITION BY job_title ORDER BY salary, id) next_salary,
    AVG(salary) OVER (PARTITION BY manager_id, job_title ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) salary_avg_5_prior,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', hire_date), level ORDER BY salary DESC, id) seq_no
FROM employees
WHERE manager_id IS NOT NULL;
