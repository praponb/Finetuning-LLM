WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        unit_price,
        MIN(qty) OVER (PARTITION BY operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_so_far,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        ROW_NUMBER() OVER (PARTITION BY operator ORDER BY qty DESC, id) AS seq_no,
        MAX(unit_price) OVER (PARTITION BY move_type) AS move_type_max_unit_price,
        ROUND(AVG(qty) OVER (PARTITION BY move_type), 2) AS move_type_avg_qty
    FROM base
),
grouped AS (
    -- Group-level totals
    SELECT
        sku_id,
        move_type,
        seq_no,
        COUNT(*) FILTER (WHERE move_type IN ('OUTBOUND', 'TRANSFER')) AS n_move_type_in_outbound_transf,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN operator IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_operator_is_not_null,
        ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM metrics
    GROUP BY sku_id, move_type, seq_no
)
SELECT
    sku_id,
    move_type,
    seq_no,
    pct_operator_is_not_null,
    avg_unit_price
FROM grouped
