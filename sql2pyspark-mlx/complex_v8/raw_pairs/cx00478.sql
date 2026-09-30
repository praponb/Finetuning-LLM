WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        ABS(qty - 348) AS qty_distance
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND move_type = 'OUTBOUND'
),
latest AS (
    -- Keep the latest row per move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        qty_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY qty DESC, id DESC) AS rn
        FROM filtered_moves
    ) ranked
    WHERE rn = 1
),
latest_2 AS (
    -- Keep the latest row per operator, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        qty_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, move_type ORDER BY unit_price DESC, id DESC) AS recency_rank
        FROM latest
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_id, sku_id, operator, qty
    FROM latest_2
) src
PIVOT (
    AVG(qty)
    FOR operator IN ('op_chan', 'op_dew', 'op_bee', 'op_eve')
)
ORDER BY warehouse_id, sku_id
