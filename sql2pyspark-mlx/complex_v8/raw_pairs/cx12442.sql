WITH filtered_plays AS (
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
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        track_id,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, session_no
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        session_end + INTERVAL 2 HOURS AS session_end_plus_2h,
        DATE_TRUNC('DAY', session_start) AS session_start_day,
        COALESCE(duration_min, 0.0) AS duration_min_filled
    FROM session_summary
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.track_id,
        s.session_no,
        s.session_start,
        s.session_end,
        s.n_events,
        s.duration_min,
        s.session_end_plus_2h,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.listener_id AS play_listener_id,
        p.device AS play_device
    FROM annotated s
    LEFT JOIN plays p ON p.track_id = s.track_id
)
SELECT *
FROM (
    SELECT session_start, play_listener_id, play_device, n_events
    FROM exploded_detail
) src
PIVOT (
    SUM(n_events)
    FOR play_device IN ('car', 'phone', 'tv', 'desktop')
)
ORDER BY session_start, play_listener_id
