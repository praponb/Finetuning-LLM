SELECT method, status, paypal_amount
FROM (
    SELECT method, status, SUM(CASE WHEN method = 'paypal' THEN amount ELSE 0 END) AS paypal_amount
    FROM payments
    GROUP BY method, status
) AS x
WHERE paypal_amount < 77
ORDER BY paypal_amount DESC, status DESC, method
LIMIT 10
