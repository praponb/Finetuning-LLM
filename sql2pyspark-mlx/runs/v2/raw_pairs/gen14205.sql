WITH ranked_employees AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) rn FROM employees) SELECT * FROM ranked_employees WHERE rn <= 5 ORDER BY email;
