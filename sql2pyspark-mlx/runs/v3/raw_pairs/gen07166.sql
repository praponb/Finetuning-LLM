SELECT monthly_fee FROM subscriptions UNION SELECT cost AS monthly_fee FROM shipments ORDER BY monthly_fee DESC
