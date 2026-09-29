SELECT * FROM (SELECT id, manager_id, hire_date, email, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) AS rn FROM employees) AS ranked WHERE rn <= 3;
