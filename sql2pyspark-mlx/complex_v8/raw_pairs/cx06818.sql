WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per sku_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS prev_moved_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
grouped AS (
    -- Group-level totals
    SELECT
        move_type,
        SUM(starts_session) AS total_starts_session,
        ROUND(AVG(session_no), 2) AS avg_session_no,
        MAX(session_no) AS max_session_no,
        MIN(session_no) AS min_session_no,
        MAX(moved_at) AS last_moved_at,
        ROUND(SUM(qty) / NULLIF(SUM(session_no), 0), 3) AS qty_to_session_no_ratio
    FROM sessionized
    GROUP BY move_type
    HAVING SUM(unit_price) > 150.7
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        move_type,
        total_starts_session,
        avg_session_no,
        max_session_no,
        min_session_no,
        last_moved_at,
        qty_to_session_no_ratio,
        ROUND(avg_session_no / NULLIF(SUM(avg_session_no) OVER w_max_session_no, 0), 4) AS avg_session_no_share_of_max_session_no,
        MAX(avg_session_no) OVER w_max_session_no AS max_session_no_max_avg_session_no,
        ROUND(PERCENT_RANK() OVER w_last_moved_at_year_total_sta, 3) AS min_session_no_percent_rank
    FROM grouped
    WINDOW w_max_session_no AS (PARTITION BY max_session_no), w_last_moved_at_year_total_sta AS (PARTITION BY YEAR(last_moved_at), total_starts_session ORDER BY min_session_no DESC, move_type)
)
SELECT
    move_type,
    total_starts_session,
    avg_session_no,
    max_session_no,
    min_session_no,
    last_moved_at,
    qty_to_session_no_ratio,
    max_session_no_max_avg_session_no,
    CASE WHEN move_type IN ('ADJUST', 'INBOUND') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
    DATEDIFF(DATE '2026-01-01', last_moved_at) AS days_since_last_moved_at
FROM ranked
ORDER BY min_session_no DESC NULLS FIRST
