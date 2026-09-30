SELECT
    order_id,
    ROUND(STDDEV(unit_price), 2) stddev_unit_price,
    MIN(quantity) min_quantity,
    SUM(quantity * unit_price) revenue
FROM order_items
GROUP BY order_id
HAVING MIN(quantity) > 3.9
ORDER BY revenue DESC
