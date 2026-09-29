SELECT id, status, paid_at, CASE WHEN method = 'card' THEN 1 ELSE 0 END is_card FROM payments WHERE method IN ('bank_transfer', 'cash');
