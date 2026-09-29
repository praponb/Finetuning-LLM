SELECT
    s.id,
    s.store_name,
    s.sqft,
    t.stddev_quantity
FROM stores s
JOIN (SELECT store_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    WHERE product_id IN (2, 5, 8)
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.stddev_quantity > 46
ORDER BY s.store_name, id
LIMIT 50
