SELECT store_id, stddev_quantity, num_inventory
FROM (
    SELECT store_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, COUNT(*) AS num_inventory
    FROM inventory
    WHERE product_id IN (5, 7)
    GROUP BY store_id
) AS sub
WHERE stddev_quantity >= 30;
