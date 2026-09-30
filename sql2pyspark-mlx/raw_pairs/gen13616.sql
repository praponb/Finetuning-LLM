WITH with_dims AS (
    -- Attach products attributes
    SELECT e.*, p.product_name
    FROM inventory e
    LEFT JOIN products p
        ON e.product_id = p.id
    WHERE p.brand NOT IN ('Acme', 'Stark', 'Initech')
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock
FROM with_dims
ORDER BY last_restock DESC NULLS LAST, quantity
