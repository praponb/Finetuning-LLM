SELECT
    d.budget,
    d.dept_name,
    d.location,
    e.job_title,
    e.hire_date,
    UPPER(e.full_name) AS full_name_upper
FROM departments d
INNER JOIN employees e ON d.id = e.dept_id
WHERE d.budget BETWEEN 1299000 AND 1904000 AND d.location = 'Bangkok';
