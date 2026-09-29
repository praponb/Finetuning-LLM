SELECT status, pct_paypal, pct_success
FROM (
    SELECT status, ROUND((100 * SUM(CASE WHEN method = 'paypal' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_paypal, ROUND((100 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_success
    FROM payments
    GROUP BY status
) AS x
WHERE pct_paypal >= 33
ORDER BY pct_success DESC, status
LIMIT 5
