SELECT start_date, 'subscription' AS source FROM subscriptions UNION ALL SELECT last_restock, 'inventory' AS source FROM inventory WHERE quantity >= 128 ORDER BY source;
