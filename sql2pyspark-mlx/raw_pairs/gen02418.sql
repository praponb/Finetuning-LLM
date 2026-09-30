WITH enriched AS (
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        AVG(salary) OVER (PARTITION BY job_title, level ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS salary_avg_6_prior,
        LEAD(salary, 2) OVER (PARTITION BY job_title ORDER BY hire_date, id) AS next_salary,
        ROW_NUMBER() OVER (ORDER BY salary DESC, id) AS seq_no
    FROM employees
)
SELECT
    s.id,
    s.manager_id,
    s.job_title,
    s.salary,
    s.is_remote,
    s.level,
    s.salary_avg_6_prior,
    s.next_salary,
    CASE WHEN level RLIKE '^(L1|L5)$' THEN 'GROUP_B' WHEN level RLIKE '^(L2|L3)$' THEN 'CORE' ELSE 'GROUP_A' END AS level_zone,
    (SELECT MIN(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS min_employee_salary
FROM enriched s
ORDER BY level_zone DESC
