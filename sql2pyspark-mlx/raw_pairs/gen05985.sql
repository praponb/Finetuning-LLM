SELECT status, paid_at, LOWER(status) AS status_lower FROM payments WHERE amount BETWEEN 620 AND 990 AND status IN ('SUCCESS', 'REFUNDED', 'FAILED') ORDER BY status
