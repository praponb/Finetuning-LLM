SELECT
    product_id,
    order_id,
    COUNT(DISTINCT quantity) unique_orders,
    COUNT(quantity) quantity_count,
    SUM(quantity * unit_price) revenue
FROM order_items
GROUP BY product_id, order_id
HAVING SUM(quantity * unit_price) > 39
ORDER BY 1
