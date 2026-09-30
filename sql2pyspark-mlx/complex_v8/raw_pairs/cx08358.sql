WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
    WHERE unit_price >= 161.4
      AND qty > 93
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        MAX(moved_at) AS last_moved_at,
        ROUND(AVG(qty), 2) AS avg_move_qty,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        st.avg_move_qty,
        COALESCE(st.move_count, 0) AS move_count
    FROM recent s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
)
SELECT id, field, value
FROM (
    SELECT id, move_count, qty_bucket
    FROM summarized_rows
) src
UNPIVOT (
    value FOR field IN (move_count AS move_count_v, qty_bucket AS qty_bucket_v)
) unp
ORDER BY id, field
