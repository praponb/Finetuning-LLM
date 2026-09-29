SELECT id, product_id
FROM order_items
WHERE quantity > 4 AND unit_price BETWEEN 240 AND 320
ORDER BY id DESC;
