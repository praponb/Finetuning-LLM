WITH recent_inventory AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (4, 5, 6) AND last_restock >= '2024-04-28'
),
product_id_summary AS (
    SELECT
        product_id,
        MONTH(last_restock) AS last_restock_month,
        COUNT(DISTINCT quantity) AS unique_products,
        MAX(quantity) - MIN(quantity) AS quantity_range
    FROM recent_inventory
    GROUP BY product_id, MONTH(last_restock)
)
SELECT *
FROM product_id_summary;
