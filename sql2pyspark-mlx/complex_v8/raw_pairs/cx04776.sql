WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LOWER(TRIM(move_type)) AS move_type_clean,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty
    FROM stock_moves
    WHERE move_type <> 'INBOUND'
),
latest_per_key AS (
    -- Keep the latest row per sku_id, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        move_type_clean,
        greatest_unit_price_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
latest AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        move_type_clean,
        greatest_unit_price_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY qty DESC, id DESC) AS recency_rank
        FROM latest_per_key
    ) ranked
    WHERE recency_rank = 1
)
SELECT id, metric_name, amount
FROM (
    SELECT id, unit_price, greatest_unit_price_qty
    FROM latest
) src
UNPIVOT (
    amount FOR metric_name IN (unit_price, greatest_unit_price_qty)
) unp
