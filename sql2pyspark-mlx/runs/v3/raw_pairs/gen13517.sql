SELECT monthly_fee, 'subscription' AS source FROM subscriptions WHERE customer_id IN (11, 32, 33, 40) UNION ALL SELECT revenue, 'store_sale' AS source FROM store_sales;
