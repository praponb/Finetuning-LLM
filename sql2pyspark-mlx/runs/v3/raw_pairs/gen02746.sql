SELECT revenue AS value, 'store_sale' AS source FROM store_sales UNION ALL SELECT monthly_fee AS value, 'subscription' AS source FROM subscriptions
