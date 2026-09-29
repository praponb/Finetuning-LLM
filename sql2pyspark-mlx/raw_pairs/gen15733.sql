SELECT *, ROUND(salary, 1) AS salary_rounded, IF(is_remote, 'Yes', 'No') AS is_remote_label FROM employees WHERE salary > 143000 AND level IN ('L2', 'L5', 'L3') AND dept_id IN (8, 22)
