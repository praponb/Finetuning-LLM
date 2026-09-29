WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC NULLS LAST, id ASC) AS row_num
    FROM inventory
    QUALIFY row_num = 1
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    row_num
FROM latest
ORDER BY last_restock DESC NULLS LAST, id DESC;
