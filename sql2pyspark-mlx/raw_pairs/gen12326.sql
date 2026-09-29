SELECT 'subscription' AS kind, monthly_fee
FROM subscriptions
UNION ALL
SELECT 'payment' AS kind, amount
FROM payments
ORDER BY monthly_fee;
