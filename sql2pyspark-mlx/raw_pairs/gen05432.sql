SELECT start_date
FROM subscriptions
WHERE monthly_fee < 240
UNION ALL
SELECT last_restock start_date
FROM inventory
ORDER BY start_date DESC;
