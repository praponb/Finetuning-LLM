WITH recent AS (
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
        LOWER(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE operator IN ('op_dew', 'op_eve', 'op_ann')
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    id,
    warehouse_id,
    sku_id,
    operator,
    move_type_clean,
    starts_session,
    SUBSTRING(TRIM(move_type_clean), 1, 4) AS move_type_clean_prefix
FROM sessionized
WHERE operator IN ('op_dew', 'op_eve', 'op_chan')
ORDER BY id DESC NULLS FIRST, warehouse_id DESC NULLS FIRST
