SELECT *, WEEKOFYEAR(hire_date) AS hire_date_weekofyear, CAST(salary AS INT) AS salary_int
FROM employees
WHERE NOT (MONTH(hire_date) IN (2, 6, 12) AND dept_id IN (8, 17) AND salary <= 107000);
