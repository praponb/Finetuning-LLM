WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per move_type
    SELECT *, LAG(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        move_type,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY move_type, burst_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(duration_min, 2) OVER (PARTITION BY burst_id ORDER BY duration_min, move_type, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_min,
        RANK() OVER (ORDER BY n_events, move_type, burst_id) AS n_events_rank,
        ROUND(burst_id / NULLIF(SUM(burst_id) OVER (PARTITION BY n_events), 0), 4) AS burst_id_share_of_n_events,
        ROUND(AVG(burst_id) OVER (PARTITION BY n_events), 2) AS n_events_avg_burst_id,
        LAST_VALUE(session_start) OVER (PARTITION BY burst_id ORDER BY duration_min, move_type, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_session_start
    FROM per_session
)
-- Final projection
SELECT
    move_type,
    burst_id,
    session_start,
    session_end,
    n_events,
    duration_min,
    second_duration_min,
    burst_id_share_of_n_events,
    n_events_avg_burst_id,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
    DATE_FORMAT(session_end, 'yyyy-MM') AS session_end_label
FROM sequenced
ORDER BY move_type_slug DESC
