WITH latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.job_title,
        x.salary
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    dept_name,
    location,
    job_title,
    CASE WHEN location RLIKE '^(Tokyo|Chiang Mai)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS location_zone
FROM latest_match
ORDER BY job_title
