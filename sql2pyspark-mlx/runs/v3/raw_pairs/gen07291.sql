WITH top_inventory AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity) rn
    FROM inventory
)
SELECT
    id,
    store_id,
    quantity,
    product_id
FROM top_inventory
WHERE rn <= 3
ORDER BY product_id DESC
