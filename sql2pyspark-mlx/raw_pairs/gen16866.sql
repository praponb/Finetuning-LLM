SELECT id, paid_at, order_id, LOWER(method) method_lower FROM payments WHERE HOUR(paid_at) BETWEEN 12 AND 19 ORDER BY 3 DESC;
