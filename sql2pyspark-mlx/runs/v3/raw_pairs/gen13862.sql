SELECT shipping_fee FROM orders WHERE channel IS NULL UNION SELECT monthly_fee AS shipping_fee FROM subscriptions WHERE customer_id = 14;
