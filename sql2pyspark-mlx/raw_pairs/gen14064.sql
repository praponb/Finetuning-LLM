SELECT 'store_sale' AS source, revenue FROM store_sales UNION SELECT 'subscription' AS source, monthly_fee FROM subscriptions WHERE customer_id = 23 ORDER BY source;
