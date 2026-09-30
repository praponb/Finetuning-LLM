SELECT shipping_fee
FROM orders
EXCEPT
SELECT unit_price AS shipping_fee
FROM order_items;
