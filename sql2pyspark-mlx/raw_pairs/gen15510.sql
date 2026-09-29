SELECT e.full_name, e.job_title, d.location FROM employees e LEFT JOIN departments d ON e.dept_id = d.id WHERE d.budget > 2853000 AND d.location NOT IN ('Bangkok', 'Chiang Mai', 'Berlin');
