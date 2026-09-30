SELECT 'subscription' AS origin, monthly_fee FROM subscriptions WHERE monthly_fee IS NOT NULL UNION ALL SELECT 'product' AS origin, cost AS monthly_fee FROM products WHERE cost IS NOT NULL
