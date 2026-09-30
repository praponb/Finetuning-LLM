WITH latest_match AS (
    SELECT
        s.id,
        s.dept_name,
        s.location,
        latest.employee_job_title,
        latest.employee_salary
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY employee_salary DESC NULLS LAST, id) AS row_num
    FROM latest_match
    QUALIFY row_num = 1
),
deduped_departments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY employee_salary DESC NULLS LAST, id) AS row_num_2
    FROM deduped
    QUALIFY row_num_2 = 1
),
eligible AS (
    SELECT s.*
    FROM deduped_departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.dept_id = 11
        )
)
SELECT
    location,
    employee_job_title,
    CASE WHEN employee_salary >= 139000 THEN 'HIGH' ELSE 'LOW' END AS employee_salary_tier,
    COUNT_IF(dept_name = 'Marketing') AS marketing_events,
    PERCENTILE_APPROX(employee_salary, 0.5) AS employee_salary_p50,
    GROUPING_ID() AS grouping_level
FROM eligible
GROUP BY CUBE (location, employee_job_title, CASE WHEN employee_salary >= 139000 THEN 'HIGH' ELSE 'LOW' END)
HAVING COUNT_IF(dept_name = 'Marketing') < 1000
ORDER BY location
