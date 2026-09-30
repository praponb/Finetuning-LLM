SELECT id, status, CONCAT(method, ', ', status) AS method_status FROM payments ORDER BY method_status DESC, id LIMIT 50;
