SELECT hire_date FROM employees UNION SELECT delivered_date AS hire_date FROM shipments WHERE shipped_date < '2024-04-01' ORDER BY hire_date DESC
