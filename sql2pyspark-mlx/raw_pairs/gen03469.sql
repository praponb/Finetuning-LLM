SELECT
    order_id,
    product_id,
    MAX(quantity) AS max_quantity,
    AVG(quantity) AS avg_quantity,
    COUNT(DISTINCT quantity) AS unique_products
FROM order_items
GROUP BY order_id, product_id
