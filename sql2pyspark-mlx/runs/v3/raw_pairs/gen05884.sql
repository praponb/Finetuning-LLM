SELECT id, status FROM payments WHERE TO_DATE(paid_at) < '2024-04-21' UNION SELECT id, status FROM payments WHERE method = 'bank_transfer' ORDER BY status;
