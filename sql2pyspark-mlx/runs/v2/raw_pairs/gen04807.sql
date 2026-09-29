SELECT
    ss.product_id,
    ss.store_id,
    AVG(i.quantity) avg_quantity,
    ROUND(STDDEV(i.quantity), 2) stddev_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.store_id IN (2, 3)
GROUP BY ss.product_id, ss.store_id
HAVING AVG(i.quantity) >= 60
ORDER BY 4;
