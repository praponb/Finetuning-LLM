WITH store_summary AS (
    SELECT store_id, MAX(quantity) AS max_quantity
    FROM inventory
    WHERE quantity < 176
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    s.opened_date,
    ss.max_quantity
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.max_quantity > 210
ORDER BY s.region DESC
