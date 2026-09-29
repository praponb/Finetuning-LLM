WITH ValidInventory AS (
    SELECT *, ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
    FROM inventory
    WHERE quantity IS NOT NULL AND (MONTH(last_restock) IN (5, 6, 8) OR product_id IN (4, 8, 9))
),
product_id_summary AS (
    SELECT product_id, MAX(last_restock) AS latest_last_restock, COUNT(DISTINCT quantity) AS unique_products
    FROM ValidInventory
    GROUP BY product_id
)
SELECT product_id, unique_products
FROM product_id_summary
WHERE unique_products > 10;
