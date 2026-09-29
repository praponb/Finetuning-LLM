SELECT dept_id, job_title, COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END) AS engineer_count
FROM employees
GROUP BY dept_id, job_title
ORDER BY dept_id DESC, engineer_count DESC
