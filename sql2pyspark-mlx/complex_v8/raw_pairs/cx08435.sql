WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM base_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        device,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        SUM(source_len) AS session_source_len,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY device, session_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        device,
        session_id,
        session_start,
        session_end,
        duration_min,
        ROUND(CUME_DIST() OVER w_events_device_by_duration_mi, 3) AS duration_min_cume_dist,
        ROUND(PERCENT_RANK() OVER w_events_device_by_duration_mi, 3) AS duration_min_percent_rank,
        COUNT(*) OVER w_device_2 AS rows_in_device
    FROM session_summary
    WINDOW w_events_device_by_duration_mi AS (PARTITION BY events, device ORDER BY duration_min), w_device_2 AS (PARTITION BY device)
)
-- Final projection
SELECT
    device,
    session_start,
    session_end,
    duration_min,
    duration_min_percent_rank,
    rows_in_device
FROM windowed
WHERE MONTH(session_start) IN (1, 2)
