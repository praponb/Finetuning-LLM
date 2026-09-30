SELECT last_restock FROM inventory EXCEPT SELECT end_date FROM subscriptions WHERE plan IN ('basic', 'premium', 'standard');
