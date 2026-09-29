WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped
    FROM plays
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        track_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, burst_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.track_id,
        s.burst_id,
        s.session_start,
        s.session_end,
        s.n_events,
        s.session_ms_played,
        t.duration_sec AS track_duration_sec,
        t.release_date AS track_release_date
    FROM sessions s
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT
    n_events,
    track_id,
    DATEDIFF(MAX(track_release_date), MIN(track_release_date)) AS track_release_date_span_days
FROM with_details
GROUP BY CUBE(n_events, track_id)
ORDER BY n_events NULLS LAST, track_id NULLS LAST
