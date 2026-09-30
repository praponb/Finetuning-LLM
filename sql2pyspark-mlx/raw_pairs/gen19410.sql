SELECT hire_date FROM employees UNION ALL SELECT delivered_date FROM shipments WHERE delivered_date BETWEEN '2024-01-01' AND '2024-10-01' ORDER BY hire_date
