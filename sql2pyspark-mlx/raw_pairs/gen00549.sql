WITH rollup_totals AS (
    -- Join a dimension and aggregate
    SELECT
        d.job_title AS employee_job_title,
        s.job_title AS employee_job_title,
        COUNT(*) AS rows_total,
        MAX(s.salary) AS max_salary,
        GROUPING_ID() AS grouping_level
    FROM employees s
    INNER JOIN employees d ON s.manager_id = d.id
    GROUP BY GROUPING SETS ((d.job_title, s.job_title), (d.job_title), ())
)
SELECT
    employee_job_title,
    employee_job_title,
    rows_total,
    max_salary,
    grouping_level,
    CASE WHEN employee_job_title RLIKE '^(Analyst|Manager)$' THEN 'EXTENDED' WHEN employee_job_title RLIKE '^(Engineer|Director)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS employee_job_title_zone
FROM rollup_totals
ORDER BY employee_job_title, max_salary;
