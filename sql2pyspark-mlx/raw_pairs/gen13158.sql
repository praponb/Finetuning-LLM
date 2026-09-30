SELECT status AS label, 'subscription' AS record_type FROM subscriptions UNION SELECT category AS label, 'product' AS record_type FROM products WHERE price <= 620;
