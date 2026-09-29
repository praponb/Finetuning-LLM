WITH recent_order_items AS (
    SELECT *
    FROM order_items
    WHERE product_id = 33 AND order_id IN (1, 12, 18, 37)
),
order_items_stats AS (
    SELECT product_id, order_id, ROUND(STDDEV(unit_price), 2) stddev_unit_price
    FROM recent_order_items
    GROUP BY product_id, order_id
)
SELECT product_id, order_id
FROM order_items_stats
ORDER BY product_id ASC, order_id DESC;
