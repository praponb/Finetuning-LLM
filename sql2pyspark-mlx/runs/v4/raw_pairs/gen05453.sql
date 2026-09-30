WITH filtered AS (
    SELECT
        *,
        ROUND(salary, 2) salary_rounded,
        CAST(is_remote AS INT) is_remote_flag
    FROM employees
    WHERE hire_date > DATE '2024-01-05'
      AND level IS NOT NULL
),
scored AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        salary_rounded,
        is_remote_flag,
        AVG(salary) OVER (PARTITION BY level ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) salary_avg_6_prior,
        LEAD(salary_rounded, 1) OVER (ORDER BY salary_rounded, id) next_salary_rounded,
        AVG(salary) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) salary_avg_3
    FROM filtered
),
with_dims AS (
    SELECT e.*, d.hire_date AS employee_hire_date
    FROM scored e
    LEFT JOIN employees d
        ON e.manager_id = d.id
),
enriched AS (
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) salary_share,
        LAST_VALUE(level, TRUE) OVER (PARTITION BY job_title ORDER BY next_salary_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_level,
        AVG(salary_avg_3) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) salary_avg_3_avg_3_prior,
        FIRST_VALUE(salary_avg_3, TRUE) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_salary_avg_3,
        LEAD(salary_avg_6_prior, 1) OVER (PARTITION BY manager_id ORDER BY hire_date, id) next_salary_avg_6_prior
    FROM with_dims
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.level,
    s.salary_rounded,
    CASE WHEN level RLIKE '^(L1|L2)$' THEN 'PRIORITY' WHEN level RLIKE '^(L3|L4)$' THEN 'CORE' ELSE 'EXTENDED' END AS level_zone,
    (SELECT MIN(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS min_employee_salary
FROM enriched s
ORDER BY id;
