WITH top_inventory AS (
    SELECT
        id,
        product_id,
        last_restock,
        store_id,
        RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) rn
    FROM inventory
    WHERE store_id = 5
)
SELECT
    id,
    product_id,
    last_restock,
    store_id
FROM top_inventory
WHERE rn <= 5
