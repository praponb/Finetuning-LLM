WITH recent_inventory AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE product_id = 10 AND quantity > 115 AND store_id IN (3, 6)
),
store_id_summary AS (
    SELECT store_id, SUM(quantity) AS total_quantity
    FROM recent_inventory
    GROUP BY store_id
)
SELECT store_id, total_quantity
FROM store_id_summary
WHERE total_quantity < 960
ORDER BY total_quantity DESC, store_id
LIMIT 20
