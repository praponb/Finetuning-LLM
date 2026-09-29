WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
),
unique_rows AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        unit_price_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY unit_price DESC, id DESC) AS row_num
        FROM base_moves
    ) ranked
    WHERE row_num = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_id, move_type, unit_price_bucket
    FROM unique_rows
) src
PIVOT (
    MAX(unit_price_bucket) AS max_v, COUNT(unit_price_bucket) AS n
    FOR move_type IN ('OUTBOUND', 'ADJUST')
)
ORDER BY warehouse_id
