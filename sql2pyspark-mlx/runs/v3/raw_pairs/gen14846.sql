SELECT last_restock FROM inventory UNION ALL SELECT end_date AS last_restock FROM subscriptions WHERE start_date > '2024-09-01' ORDER BY last_restock
