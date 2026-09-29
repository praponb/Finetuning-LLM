SELECT monthly_fee, 'subscription' AS source
FROM subscriptions
UNION ALL
SELECT revenue, 'store_sale' AS source
FROM store_sales;
