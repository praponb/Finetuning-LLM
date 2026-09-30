WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE store_id = 4
)
-- Deduplicate with ROW_NUMBER
SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock DESC NULLS FIRST, id) AS row_num
FROM base
QUALIFY row_num = 1;
