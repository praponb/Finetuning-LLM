SELECT monthly_fee
FROM subscriptions
EXCEPT
SELECT unit_price
FROM order_items
ORDER BY monthly_fee DESC
