WITH store_stats AS (
    SELECT store_id, COUNT(*) AS total_count, COUNT(*) AS inventory_count
    FROM inventory
    GROUP BY store_id
)
SELECT
    stores.id,
    stores.city,
    store_stats.total_count,
    store_stats.inventory_count
FROM stores
LEFT OUTER JOIN store_stats ON stores.id = store_stats.store_id
WHERE store_stats.inventory_count > 7
ORDER BY stores.id DESC
