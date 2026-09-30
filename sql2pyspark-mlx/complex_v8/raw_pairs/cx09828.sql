WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d
    FROM stock_moves
    WHERE unit_price > 189.4
      AND operator IS NOT NULL
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM cleaned s
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type, operator)) AS distinct_sku_id_in_move_type_operator,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_plus_7d), sku_id ORDER BY qty, id) AS qty_quintile,
        move_count - LAG(move_count) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at_plus_7d), sku_id ORDER BY qty, id) AS move_count_change
    FROM with_counts
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT sku_id, move_type, qty_quintile
    FROM windowed
) src
PIVOT (
    SUM(qty_quintile)
    FOR move_type IN ('TRANSFER', 'ADJUST', 'OUTBOUND', 'INBOUND')
)
ORDER BY sku_id
