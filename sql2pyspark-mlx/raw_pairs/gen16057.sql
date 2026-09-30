SELECT 'payment' AS source, method FROM payments UNION SELECT 'department' AS source, location FROM departments ORDER BY source DESC
