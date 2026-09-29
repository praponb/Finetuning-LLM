WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        moved_at + INTERVAL 1 DAYS AS moved_at_plus_1d,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        FLOOR(qty / 100) * 100 AS qty_bucket_2
    FROM stock_moves
    WHERE move_type = 'ADJUST'
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) AS move_count,
        COUNT(*) AS move_count_2,
        SUM(qty) AS total_move_qty
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2,
        st.total_move_qty
    FROM recent s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        HOUR(moved_at_plus_1d) AS moved_at_plus_1d_hour,
        COALESCE(qty, 0) AS qty_filled,
        ROUND(total_move_qty / NULLIF(move_count, 0), 3) AS total_move_qty_per_move_count
    FROM with_stats
    WHERE move_count > 1
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    operator,
    unit_price,
    qty_bucket_2,
    move_count,
    move_count_2,
    total_move_qty,
    moved_at_plus_1d_hour,
    qty_filled,
    total_move_qty_per_move_count
FROM enriched
ORDER BY unit_price DESC, total_move_qty_per_move_count NULLS LAST, id
LIMIT 20
