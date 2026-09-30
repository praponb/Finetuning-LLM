SELECT monthly_fee, 'subscription' source FROM subscriptions UNION SELECT revenue, 'store_sale' source FROM store_sales ORDER BY source;
