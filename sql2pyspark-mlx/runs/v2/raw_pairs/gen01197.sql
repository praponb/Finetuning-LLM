WITH numbered AS (
    SELECT
        id,
        product_id,
        last_restock,
        store_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS rnk
    FROM inventory
)
SELECT *
FROM numbered
WHERE rnk <= 5;
