WITH store_summary AS (
    SELECT store_id, MIN(last_restock) AS min_last_restock
    FROM inventory
    WHERE product_id = 9
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    s.region,
    ss.min_last_restock
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id;
