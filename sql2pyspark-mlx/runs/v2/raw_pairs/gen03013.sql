SELECT is_remote, MIN(hire_date) first_hire_date
FROM employees
WHERE hire_date BETWEEN '2023-03-08' AND '2025-10-01' OR hire_date < '2025-10-01'
GROUP BY is_remote
ORDER BY first_hire_date;
