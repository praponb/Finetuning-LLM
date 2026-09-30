WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-24 08:00:00'
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    operator,
    CONCAT(operator, '#', CAST(id AS STRING)) AS operator_ref
FROM valid
ORDER BY id DESC
