WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE qty <= 384
      AND moved_at < TIMESTAMP '2025-02-12 15:00:00'
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at
    FROM scoped_moves s
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        move_type,
        operator,
        unit_price,
        last_moved_at,
        LENGTH(TRIM(move_type_or_unknown)) AS move_type_or_unknown_len,
        ROUND((UNIX_TIMESTAMP(last_moved_at) - UNIX_TIMESTAMP(moved_at_year)) / 3600, 1) AS hours_moved_at_year_to_last_moved_at
    FROM with_counts
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM scored s
)
SELECT *
FROM (
    SELECT move_count, last_moved_at, move_type, move_type_or_unknown_len
    FROM profiled
) src
PIVOT (
    COUNT(move_type_or_unknown_len)
    FOR move_type IN ('OUTBOUND', 'TRANSFER', 'INBOUND', 'ADJUST')
)
ORDER BY move_count, last_moved_at
