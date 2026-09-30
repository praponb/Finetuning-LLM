WITH inventory_by_store AS (
    SELECT store_id, MAX(last_restock) AS latest_last_restock, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    WHERE product_id IN (5, 6)
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    ibs.latest_last_restock,
    ibs.stddev_quantity
FROM stores s
LEFT OUTER JOIN inventory_by_store ibs ON s.id = ibs.store_id
WHERE ibs.stddev_quantity > 55
ORDER BY latest_last_restock DESC;
