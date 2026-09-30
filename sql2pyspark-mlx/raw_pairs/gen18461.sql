SELECT shipping_fee AS value FROM orders WHERE shipping_fee IS NOT NULL UNION SELECT monthly_fee AS value FROM subscriptions WHERE monthly_fee IS NOT NULL
