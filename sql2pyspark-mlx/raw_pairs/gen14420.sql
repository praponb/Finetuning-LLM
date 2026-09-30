WITH filtered_order_items AS (
    SELECT *
    FROM order_items
    WHERE (unit_price < 210 AND quantity > 8) OR unit_price BETWEEN 130 AND 280
),
order_items_stats AS (
    SELECT product_id, MAX(quantity) AS max_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM filtered_order_items
    GROUP BY product_id
)
SELECT product_id, max_quantity, stddev_quantity
FROM order_items_stats
WHERE max_quantity > 4.5
ORDER BY max_quantity;
