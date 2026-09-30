SELECT id, paid_at, CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END AS is_failed
FROM payments
