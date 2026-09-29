SELECT
    d.location,
    COUNT(e.id) AS employee_count,
    MAX(e.hire_date) AS latest_hire_date,
    SORT_ARRAY(COLLECT_SET(e.job_title)) AS job_title_list
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.level IN ('L3', 'L5')
GROUP BY d.location
ORDER BY employee_count ASC
