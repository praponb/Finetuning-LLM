SELECT
    order_id,
    SUM(quantity) AS total_quantity,
    COUNT(*) AS cnt,
    SUM(SUM(quantity)) OVER (ORDER BY order_id) AS cumulative_quantity
FROM order_items
GROUP BY order_id;
