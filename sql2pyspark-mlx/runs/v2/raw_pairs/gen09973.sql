SELECT *, LOWER(status) AS status_lower
FROM payments
WHERE amount <= 1200 AND status = 'SUCCESS'
