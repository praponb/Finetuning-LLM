SELECT *, CAST(salary AS INT) AS salary_int, CONCAT(full_name, ' ', level) AS full_name_level
FROM employees
ORDER BY 10 ASC, id ASC;
