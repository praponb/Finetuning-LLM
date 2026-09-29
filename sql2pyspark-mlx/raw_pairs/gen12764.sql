WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        last_restock,
        CAST(quantity AS STRING) AS quantity_str,
        DATE_ADD(last_restock, 71) AS last_restock_plus_71d
    FROM inventory
    WHERE last_restock > DATE '2023-05-20'
      AND store_id IN (1, 2)
)
SELECT product_id, id
FROM valid
EXCEPT
SELECT product_id, id
FROM inventory
WHERE quantity > 195;
