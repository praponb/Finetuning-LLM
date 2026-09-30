SELECT last_restock, 'inventory' AS source FROM inventory WHERE last_restock > '2025-01-01' UNION SELECT start_date, 'subscription' AS source FROM subscriptions
