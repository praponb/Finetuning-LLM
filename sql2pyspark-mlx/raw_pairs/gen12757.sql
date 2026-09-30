SELECT 'subscription' AS origin, monthly_fee FROM subscriptions UNION SELECT 'order' AS origin, shipping_fee FROM orders WHERE customer_id = 19
