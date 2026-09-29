WITH Numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) AS rnk FROM employees WHERE hire_date >= '2023-06-01') SELECT * FROM Numbered WHERE rnk <= 2;
