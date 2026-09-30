SELECT status, 'payment' source
FROM payments
WHERE status = 'SUCCESS'
UNION ALL
SELECT status, 'subscription' source
FROM subscriptions
WHERE YEAR(end_date) >= 2023
