SELECT
    id,
    full_name,
    salary,
    NTILE(10) OVER (PARTITION BY level ORDER BY salary DESC, id) salary_bucket,
    COUNT(salary) OVER w_grp hire_date_year_count_salary,
    ROUND(salary / SUM(salary) OVER (PARTITION BY DATE_TRUNC('WEEK', hire_date), level), 4) salary_share,
    MAX(salary) OVER (PARTITION BY manager_id) manager_id_max_salary,
    FIRST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_job_title
FROM employees
WINDOW w_grp AS (PARTITION BY YEAR(hire_date), job_title);
