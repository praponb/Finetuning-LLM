WITH ranked_employees AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date, id) rnk FROM employees) SELECT id, job_title, hire_date, email FROM ranked_employees WHERE rnk <= 3
