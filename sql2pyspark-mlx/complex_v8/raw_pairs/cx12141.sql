WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price_2
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND operator NOT IN ('op_chan', 'op_eve')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty_per_unit_price,
        qty_per_unit_price_2,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        RANK() OVER (PARTITION BY move_type, operator ORDER BY qty_per_unit_price) AS qty_per_unit_price_rank,
        LEAD(qty_per_unit_price_2, 1, 0) OVER (PARTITION BY move_type, operator ORDER BY qty_per_unit_price, id) AS next_qty_per_unit_price_2,
        FIRST_VALUE(moved_at) OVER (PARTITION BY move_type, operator ORDER BY qty_per_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at
    FROM base
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(qty_per_unit_price_2, 0.0) AS qty_per_unit_price_2_filled,
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM metrics
)
SELECT
    id,
    warehouse_id,
    move_type,
    rows_in_warehouse_id,
    qty_per_unit_price_rank,
    next_qty_per_unit_price_2,
    first_moved_at,
    qty_per_unit_price_2_filled,
    move_type_len,
    CASE WHEN move_type IN ('TRANSFER', 'ADJUST') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
FROM enriched
ORDER BY qty_per_unit_price_rank NULLS LAST
