SELECT id, order_id
FROM order_items
WHERE product_id IN (1, 4, 30)
UNION
SELECT id, order_id
FROM order_items
WHERE unit_price > 260;
