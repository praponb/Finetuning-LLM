SELECT status, COALESCE(method, 'Unknown') method_filled, HOUR(paid_at) paid_at_hour
FROM payments
WHERE status IN ('SUCCESS', 'REFUNDED', 'FAILED') AND amount >= 1150
ORDER BY status
