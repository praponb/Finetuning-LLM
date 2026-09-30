SELECT order_id FROM payments WHERE paid_at >= '2025-10-01' UNION ALL SELECT order_id FROM payments WHERE amount IS NOT NULL ORDER BY order_id;
