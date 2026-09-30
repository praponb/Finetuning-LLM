SELECT id, hire_date, ROW_NUMBER() OVER (ORDER BY hire_date, id) rn
FROM employees
WHERE salary >= 68000;
