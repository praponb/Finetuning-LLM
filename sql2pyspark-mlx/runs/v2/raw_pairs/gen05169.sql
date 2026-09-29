SELECT *
FROM (
    SELECT product_id, ROUND(STDDEV(quantity), 2) stddev_quantity, ROUND(AVG(quantity), 2) avg_quantity
    FROM inventory
    GROUP BY product_id
) agg
WHERE stddev_quantity < 75
ORDER BY 1 DESC
LIMIT 5;
