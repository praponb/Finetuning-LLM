WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(burst_id / NULLIF(days_since_moved_at, 0), 3) AS burst_id_per_days_since_moved_at,
        LENGTH(TRIM(move_type)) AS move_type_len,
        LOWER(TRIM(move_type)) AS move_type_clean
    FROM sessionized
    WHERE HOUR(moved_at) BETWEEN 10 AND 14
)
SELECT
    id,
    sku_id,
    move_type,
    moved_at,
    days_since_moved_at,
    prev_moved_at,
    starts_session,
    burst_id_per_days_since_moved_at,
    move_type_len,
    move_type_clean,
    DATEDIFF(DATE '2025-06-30', prev_moved_at) AS days_since_prev_moved_at,
    CASE WHEN operator IN ('op_bee', 'op_ann') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group
FROM scored
WHERE move_type_len < 10
ORDER BY move_type NULLS LAST, move_type_len
