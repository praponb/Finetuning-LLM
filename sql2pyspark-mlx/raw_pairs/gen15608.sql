SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS rnk
    FROM employees
) AS r
WHERE rnk <= 3
