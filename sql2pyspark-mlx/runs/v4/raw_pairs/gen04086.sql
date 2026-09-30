WITH valid AS (
    SELECT
        id,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        CASE WHEN salary >= 149000 THEN 'High' WHEN salary >= 53000 THEN 'Medium' ELSE 'Low' END salary_band
    FROM employees
    WHERE hire_date >= TO_DATE('2023-03-12')
),
with_windows AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY job_title, dept_id ORDER BY salary DESC, id) salary_bucket,
        AVG(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) salary_avg_3,
        NTILE(3) OVER (PARTITION BY dept_id, level ORDER BY salary DESC, id) salary_bucket_2
    FROM valid
),
valid_employees AS (
    SELECT *, CONCAT(job_title, '/', level) job_title_level
    FROM with_windows
    WHERE hire_date > TO_DATE('2024-02-27')
      AND level IS NOT NULL
      AND is_remote
),
latest AS (
    SELECT *
    FROM valid_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id, level ORDER BY salary_bucket DESC, id) = 1
)
SELECT
    s.id,
    s.dept_id,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    s.salary_band,
    s.salary_avg_3,
    s.job_title_level,
    CASE WHEN level RLIKE '^(L5|L1)$' THEN 'CORE' WHEN level RLIKE '^(L2|L3)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS level_zone,
    (SELECT SUM(e2.salary) FROM employees e2 WHERE e2.dept_id = s.dept_id) AS sum_employee_salary
FROM latest s
ORDER BY dept_id
