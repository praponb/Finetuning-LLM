SELECT status
FROM orders
WHERE shipping_fee BETWEEN 29 AND 39
UNION ALL
SELECT status
FROM subscriptions
WHERE end_date > '2025-02-01'
ORDER BY status;
