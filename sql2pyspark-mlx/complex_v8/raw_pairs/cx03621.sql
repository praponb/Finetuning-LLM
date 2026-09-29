WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM valid_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        sku_id,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY sku_id, session_id
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.sku_id,
        s.session_id,
        s.session_start,
        s.session_end,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active
    FROM per_session s
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    sku_id,
    session_id,
    sku_labels,
    sku_is_active,
    DATE_TRUNC('QUARTER', session_start) AS session_start_quarter
FROM combined_details
WHERE NOT sku_is_active
ORDER BY session_id DESC, session_start_quarter DESC
