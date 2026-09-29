SELECT id, status FROM payments WHERE status IN ('REFUNDED', 'FAILED') OR amount IS NULL;
