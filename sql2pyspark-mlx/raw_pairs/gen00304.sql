WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS rnk
    FROM inventory
)
SELECT
    id,
    product_id,
    last_restock,
    store_id
FROM numbered
WHERE rnk <= 5
