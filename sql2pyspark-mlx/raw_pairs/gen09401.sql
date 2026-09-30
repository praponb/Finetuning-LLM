SELECT 'subscription' AS origin, plan FROM subscriptions UNION ALL SELECT 'supplier' AS origin, country AS plan FROM suppliers;
