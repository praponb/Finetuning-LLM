SELECT id, method, LENGTH(status) AS status_length
FROM payments
WHERE status IN ('SUCCESS', 'FAILED') AND status = 'SUCCESS' AND status = 'REFUNDED'
