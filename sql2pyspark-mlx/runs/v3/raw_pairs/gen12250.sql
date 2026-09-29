WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (2, 3, 5, 6) AND quantity IS NULL AND MONTH(last_restock) IN (2, 4, 8)
),
product_id_summary AS (
    SELECT product_id, MONTH(last_restock) AS last_restock_month, MAX(last_restock) AS last_last_restock
    FROM valid_inventory
    GROUP BY product_id, MONTH(last_restock)
)
SELECT product_id, last_restock_month, last_last_restock
FROM product_id_summary
