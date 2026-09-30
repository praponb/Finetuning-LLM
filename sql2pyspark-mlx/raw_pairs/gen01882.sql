SELECT
    order_id,
    ROUND(STDDEV(unit_price), 2) AS stddev_unit_price,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    SUM(quantity * unit_price) AS revenue
FROM order_items
WHERE quantity <= 3
GROUP BY order_id
ORDER BY order_id DESC;
