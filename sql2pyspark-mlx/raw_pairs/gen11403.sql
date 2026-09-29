WITH scored AS (
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        is_remote,
        level,
        NTILE(10) OVER (PARTITION BY YEAR(hire_date), job_title ORDER BY salary DESC, id) AS salary_bucket,
        FIRST_VALUE(job_title, TRUE) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share_2,
        PERCENT_RANK() OVER (PARTITION BY MONTH(hire_date), job_title ORDER BY salary) AS salary_percent_rank
    FROM employees
)
SELECT
    s.id,
    s.manager_id,
    s.salary_bucket,
    s.first_known_job_title,
    s.salary_share,
    s.salary_percent_rank,
    (SELECT MAX(x.salary) FROM employees x WHERE x.manager_id = s.manager_id) AS max_employee_salary
FROM scored s
ORDER BY salary_bucket;
