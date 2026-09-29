SELECT last_restock, 'inventory' AS source FROM inventory UNION ALL SELECT start_date AS last_restock, 'subscription' AS source FROM subscriptions
