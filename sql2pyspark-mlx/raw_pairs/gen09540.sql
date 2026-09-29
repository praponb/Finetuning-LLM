SELECT end_date FROM subscriptions EXCEPT SELECT launch_date FROM products WHERE SIZE(tags) > 2
