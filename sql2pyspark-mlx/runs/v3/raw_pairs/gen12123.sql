SELECT status
FROM payments
WHERE status != 'FAILED'
UNION ALL
SELECT status
FROM subscriptions
WHERE end_date BETWEEN '2023-04-01' AND '2023-11-01'
ORDER BY status DESC;
