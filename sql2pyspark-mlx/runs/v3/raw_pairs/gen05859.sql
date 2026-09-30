SELECT status, method, failed_count
FROM (
    SELECT status, method, SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
    FROM payments
    WHERE method NOT IN ('paypal', 'promptpay')
    GROUP BY status, method
) AS s
WHERE failed_count > 46
