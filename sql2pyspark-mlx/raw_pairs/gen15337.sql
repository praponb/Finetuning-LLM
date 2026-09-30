WITH store_summary AS (
    SELECT store_id, COUNT(*) AS num_inventory, SUM(quantity) AS total_quantity
    FROM inventory
    GROUP BY store_id
)
SELECT s.opened_date, ss.num_inventory, ss.total_quantity
FROM stores s
LEFT OUTER JOIN store_summary ss ON s.id = ss.store_id
ORDER BY opened_date;
