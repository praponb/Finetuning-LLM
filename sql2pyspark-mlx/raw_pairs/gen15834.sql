SELECT
    d.location,
    d.budget,
    d.dept_name,
    e.manager_id,
    CASE WHEN e.job_title = 'Designer' THEN 1 ELSE 0 END AS is_designer
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE e.salary BETWEEN 70000 AND 76000
ORDER BY 5 DESC, 4 DESC
