WITH with_latest AS (
    SELECT
        s.*,
        top1.job_title,
        top1.hire_date
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    location,
    SUM(CASE WHEN location = 'New York' THEN budget ELSE 0 END) AS new_york_budget,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    COUNT(DISTINCT dept_name) AS distinct_dept_names
FROM with_latest
GROUP BY 1
