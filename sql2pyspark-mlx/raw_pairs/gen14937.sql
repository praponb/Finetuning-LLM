SELECT job_title, is_remote, IF(is_remote, 'Yes', 'No') AS is_remote_label
FROM employees
WHERE dept_id IN (5, 22) OR hire_date BETWEEN '2023-08-01' AND '2023-10-01'
ORDER BY job_title
