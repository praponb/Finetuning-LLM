WITH deduped AS (SELECT * FROM employees QUALIFY RANK() OVER (PARTITION BY dept_id ORDER BY salary) = 1) SELECT id, email, job_title, salary, hire_date, level FROM deduped
