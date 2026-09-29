SELECT 'store' AS record_type, city FROM stores WHERE city IS NOT NULL UNION ALL SELECT 'subscription' AS record_type, plan FROM subscriptions WHERE plan IS NOT NULL;
