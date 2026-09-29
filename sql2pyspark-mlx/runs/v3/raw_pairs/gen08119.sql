SELECT last_restock FROM inventory EXCEPT SELECT end_date FROM subscriptions WHERE start_date < '2025-08-01';
