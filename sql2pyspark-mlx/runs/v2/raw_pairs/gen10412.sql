WITH recent_inventory AS (
    SELECT *, ROUND(quantity, 2) AS quantity_rounded
    FROM inventory
    WHERE store_id = 1 OR store_id = 6
),
product_id_summary AS (
    SELECT
        product_id,
        store_id,
        MAX(last_restock) AS latest_last_restock,
        COUNT(DISTINCT quantity) AS unique_stores
    FROM recent_inventory
    GROUP BY product_id, store_id
)
SELECT
    product_id,
    store_id,
    latest_last_restock,
    unique_stores
FROM product_id_summary
WHERE unique_stores < 6
ORDER BY store_id DESC, product_id
LIMIT 1;
