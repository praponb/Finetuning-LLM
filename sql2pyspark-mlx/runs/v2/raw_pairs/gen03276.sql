SELECT id, method, COUNT(*) OVER (PARTITION BY method) AS method_count FROM payments WHERE paid_at >= '2023-12-27';
