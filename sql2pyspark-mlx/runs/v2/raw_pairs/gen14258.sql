SELECT id, dept_id, full_name
FROM employees
WHERE level NOT IN ('L4', 'L5') AND (hire_date <= '2024-09-04' OR job_title <> 'Analyst');
