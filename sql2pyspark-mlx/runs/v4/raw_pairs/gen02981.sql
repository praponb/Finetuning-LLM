WITH recent_employees AS (
    SELECT *, COALESCE(job_title, 'Unknown') AS job_title_filled
    FROM employees
    WHERE salary <= 96000 AND level = 'L5' AND level IN ('L5', 'L4')
),
IsRemoteSummary AS (
    SELECT is_remote, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, MAX(hire_date) AS latest_hire_date
    FROM recent_employees
    GROUP BY is_remote
)
SELECT *
FROM IsRemoteSummary
