WITH valid AS (
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
        CASE WHEN source IN ('search', 'playlist') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        source,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, burst_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(burst_id, 3) OVER w_burst_id_by_n_events AS next3_burst_id,
        COUNT(*) OVER (PARTITION BY source, n_events) AS rows_in_source_n_events
    FROM session_summary
    WINDOW w_burst_id_by_n_events AS (PARTITION BY burst_id ORDER BY n_events DESC, source, burst_id)
)
-- Final projection
SELECT
    source,
    burst_id,
    session_start,
    session_end,
    duration_min,
    next3_burst_id,
    rows_in_source_n_events,
    DATEDIFF(DATE '2026-01-01', session_start) AS days_since_session_start,
    COALESCE(next3_burst_id, 0) AS next3_burst_id_filled
FROM windowed
ORDER BY rows_in_source_n_events, session_start
