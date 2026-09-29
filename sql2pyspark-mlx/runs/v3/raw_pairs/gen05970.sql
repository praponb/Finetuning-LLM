SELECT monthly_fee AS value, 'subscription' AS source FROM subscriptions WHERE plan <> 'basic' UNION ALL SELECT amount AS value, 'order' AS source FROM orders
