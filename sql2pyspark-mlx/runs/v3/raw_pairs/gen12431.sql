SELECT status, paid_at, method, CONCAT_WS('/', method, status) AS method_status_key FROM payments ORDER BY method_status_key;
