SELECT
    d.dept_name,
    COUNT(e.id) AS employee_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(e.job_title))) AS job_title_values,
    SUM(CASE WHEN e.level = 'L4' THEN 1 ELSE 0 END) AS l4_count
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
WHERE e.hire_date <= '2024-02-01'
GROUP BY d.dept_name
