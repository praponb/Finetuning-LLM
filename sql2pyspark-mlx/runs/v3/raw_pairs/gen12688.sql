SELECT unit_price FROM order_items WHERE order_id IN (2, 21, 26, 31) EXCEPT SELECT monthly_fee FROM subscriptions ORDER BY unit_price
