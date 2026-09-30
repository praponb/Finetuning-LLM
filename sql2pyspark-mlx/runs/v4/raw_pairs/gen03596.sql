SELECT d.location, e.manager_id, e.is_remote, e.job_title FROM departments d INNER JOIN employees e ON e.dept_id = d.id WHERE d.location IN ('Tokyo', 'London', 'New York') AND d.budget IS NULL;
