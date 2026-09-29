SELECT d.location, SUM(CASE WHEN e.job_title = 'Engineer' THEN 1 ELSE 0 END) AS engineer_count
FROM employees e
JOIN departments d ON d.id = e.dept_id
WHERE d.location IN ('Chiang Mai', 'Bangkok')
GROUP BY d.location
