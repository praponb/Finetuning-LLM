SELECT status FROM payments WHERE paid_at < '2025-08-01' UNION ALL SELECT status FROM payments WHERE method <> 'paypal' ORDER BY status DESC
