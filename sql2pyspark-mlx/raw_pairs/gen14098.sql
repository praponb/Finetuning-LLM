SELECT 'subscription' AS origin, monthly_fee FROM subscriptions WHERE plan = 'enterprise' UNION SELECT 'store_sale' AS origin, revenue FROM store_sales WHERE units <> 23 ORDER BY monthly_fee;
