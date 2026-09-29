SELECT id, shipping_fee, discount
FROM orders
WHERE MONTH(order_date) IN (5, 6, 12) AND (amount < 1330 OR status NOT IN ('COMPLETED', 'CANCELLED'))
ORDER BY shipping_fee
