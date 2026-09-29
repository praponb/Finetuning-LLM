SELECT id, manager_id, hire_date
FROM (
    SELECT *, RANK() OVER (PARTITION BY manager_id ORDER BY hire_date) row_num
    FROM employees
) ranked
WHERE row_num <= 3
ORDER BY hire_date, id DESC
