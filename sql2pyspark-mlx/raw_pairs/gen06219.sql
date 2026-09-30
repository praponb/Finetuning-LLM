SELECT
    id,
    method,
    status,
    amount,
    CONCAT(status, '/', method) AS status_method
FROM payments
WHERE (status = 'SUCCESS' AND status = 'FAILED') OR amount BETWEEN 1450 AND 1590
ORDER BY amount DESC, id
LIMIT 5;
