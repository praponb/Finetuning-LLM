SELECT product_id, stddev_quantity, row_count
FROM (
    SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, COUNT(*) AS row_count
    FROM order_items
    GROUP BY product_id
) AS t
WHERE stddev_quantity >= 27
ORDER BY stddev_quantity
