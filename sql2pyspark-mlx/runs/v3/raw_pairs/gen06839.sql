SELECT start_date, 'subscription' AS source
FROM subscriptions
WHERE monthly_fee < 220
UNION ALL
SELECT last_restock, 'inventory' AS source
FROM inventory
WHERE quantity <= 239;
