SELECT
    order_items.id,
    order_items.order_id,
    order_items.unit_price,
    products.brand
FROM order_items
RIGHT JOIN products ON products.id = order_items.product_id
WHERE products.price BETWEEN 320 AND 440
ORDER BY order_id DESC;
