SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC NULLS LAST, id) AS latest_rank
FROM employees
QUALIFY latest_rank = 1
