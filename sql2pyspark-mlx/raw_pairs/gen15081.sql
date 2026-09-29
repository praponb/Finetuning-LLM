SELECT *, COALESCE(method, 'unassigned') AS method_filled FROM payments WHERE HOUR(paid_at) BETWEEN 8 AND 11 ORDER BY order_id DESC;
