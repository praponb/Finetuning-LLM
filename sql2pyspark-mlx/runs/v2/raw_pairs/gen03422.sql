SELECT id, paid_at, SUBSTRING(method, 1, 3) AS method_prefix FROM payments WHERE order_id IS NOT NULL ORDER BY id DESC, paid_at DESC
