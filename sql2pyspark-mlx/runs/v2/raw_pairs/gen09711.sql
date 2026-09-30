SELECT *
FROM (
    SELECT
        store_id,
        product_id,
        ROUND(STDDEV(quantity), 2) AS stddev_quantity,
        MIN(last_restock) AS min_last_restock
    FROM inventory
    WHERE product_id IN (1, 3, 4, 9)
    GROUP BY store_id, product_id
) AS s
WHERE stddev_quantity > 31
ORDER BY stddev_quantity DESC;
