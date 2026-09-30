SELECT
    id,
    order_id,
    product_id,
    quantity,
    COUNT(*) OVER (PARTITION BY order_id) AS order_id_count,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS rn
FROM order_items
WHERE order_id IN (17, 23, 38);
