SELECT 'order_item' AS record_type, unit_price
FROM order_items
WHERE unit_price <= 260
UNION ALL
SELECT 'subscription' AS record_type, monthly_fee
FROM subscriptions
ORDER BY record_type DESC;
