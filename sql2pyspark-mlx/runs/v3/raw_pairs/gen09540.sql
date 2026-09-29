SELECT
    d.id,
    d.dept_name,
    COUNT(e.id) employee_count,
    SORT_ARRAY(COLLECT_SET(e.job_title)) job_title_list,
    SORT_ARRAY(COLLECT_SET(e.level)) level_list
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id
GROUP BY d.id, d.dept_name
ORDER BY d.id DESC;
