WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        YEAR(moved_at) AS moved_at_year
    FROM stock_moves
    WHERE qty >= 189
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        moved_at_year,
        prev_moved_at,
        new_session_flag,
        session_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY new_session_flag DESC, id DESC) AS row_num
        FROM sessionized
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    moved_at,
    operator,
    session_id
FROM latest
ORDER BY qty, move_type DESC, id
LIMIT 20
