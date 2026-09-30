WITH latest_match AS (
    SELECT
        s.id,
        s.dept_name,
        top1.job_title,
        top1.salary
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    dept_name,
    job_title,
    ROUND(AVG(salary), 2) AS avg_salary,
    SUM(salary) FILTER (WHERE dept_name = 'Engineering') AS engineering_total_salary,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    PERCENTILE_APPROX(salary, 0.5) AS salary_p50,
    COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count
FROM latest_match
GROUP BY dept_name, job_title
