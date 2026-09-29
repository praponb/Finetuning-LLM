SELECT monthly_fee AS value
FROM subscriptions
WHERE monthly_fee IS NOT NULL
UNION ALL
SELECT cost AS value
FROM shipments
WHERE cost IS NOT NULL;
