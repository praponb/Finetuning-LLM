WITH top_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) rnk
    FROM inventory
)
SELECT
    id,
    store_id,
    quantity,
    product_id
FROM top_inventory
WHERE rnk <= 2
