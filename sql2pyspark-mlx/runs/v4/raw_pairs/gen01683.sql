SELECT unit_price
FROM order_items
WHERE product_id IS NOT NULL
INTERSECT
SELECT shipping_fee
FROM orders
