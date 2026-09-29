SELECT order_id, stddev_unit_price, min_quantity
FROM (
    SELECT order_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price, MIN(quantity) AS min_quantity
    FROM order_items
    GROUP BY order_id
) AS agg
WHERE min_quantity < 5.5
ORDER BY min_quantity DESC, order_id
LIMIT 50
