WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        ABS(qty - 300) AS qty_distance
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM valid_moves
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        operator,
        session_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS events,
        SUM(qty) AS session_qty,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        operator,
        session_id,
        events,
        session_qty,
        LAG(session_end, 3) OVER (PARTITION BY events, operator ORDER BY events, operator, session_id) AS prev3_session_end,
        ROUND(CUME_DIST() OVER (PARTITION BY events, operator ORDER BY events), 3) AS events_cume_dist,
        FIRST_VALUE(duration_min) OVER (PARTITION BY events, operator ORDER BY events, operator, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_min,
        LAST_VALUE(session_end) OVER (PARTITION BY events, operator ORDER BY events, operator, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_session_end,
        NTILE(10) OVER (PARTITION BY events, operator ORDER BY events, operator, session_id) AS events_decile
    FROM session_summary
)
-- Final projection
SELECT
    operator,
    session_id,
    events,
    session_qty,
    prev3_session_end,
    events_cume_dist,
    first_duration_min,
    last_session_end,
    events + events_cume_dist AS events_plus_events_cume_dist
FROM sequenced
WHERE events_decile BETWEEN 1 AND 7
ORDER BY first_duration_min, operator, session_id
LIMIT 5
