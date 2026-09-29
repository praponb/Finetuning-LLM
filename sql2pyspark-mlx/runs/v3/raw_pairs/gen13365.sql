WITH base AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (1, 2) AND store_id IN (2, 4, 6) AND MONTH(last_restock) IN (2, 3, 8)
),
inventory_stats AS (
    SELECT
        product_id,
        DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month,
        MAX(quantity) AS max_quantity,
        MIN(last_restock) AS first_last_restock
    FROM base
    GROUP BY product_id, DATE_FORMAT(last_restock, 'yyyy-MM')
)
SELECT
    product_id,
    last_restock_month,
    max_quantity,
    first_last_restock
FROM inventory_stats
WHERE max_quantity > 160
ORDER BY last_restock_month DESC;
