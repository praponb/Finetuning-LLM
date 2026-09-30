SELECT shipping_fee FROM orders WHERE shipping_fee >= 37 UNION SELECT monthly_fee AS shipping_fee FROM subscriptions;
