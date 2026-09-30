WITH store_stats AS (
    SELECT store_id, MAX(last_restock) last_last_restock, ROUND(STDDEV(quantity), 2) stddev_quantity
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.opened_date,
    s.city,
    ss.last_last_restock,
    ss.stddev_quantity
FROM stores s
LEFT JOIN store_stats ss ON s.id = ss.store_id
WHERE ss.stddev_quantity >= 37
ORDER BY s.city
