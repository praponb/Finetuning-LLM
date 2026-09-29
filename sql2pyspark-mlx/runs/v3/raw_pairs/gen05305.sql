SELECT price FROM products WHERE cost < 690 UNION ALL SELECT monthly_fee AS price FROM subscriptions;
