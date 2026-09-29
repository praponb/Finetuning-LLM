SELECT end_date, 'subscription' AS source
FROM subscriptions
WHERE monthly_fee > 100
UNION ALL
SELECT last_restock, 'inventory' AS source
FROM inventory
WHERE quantity <= 84
ORDER BY source;
