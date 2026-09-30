WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour
    FROM plays
    WHERE source IN ('playlist', 'search')
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM recent_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        listener_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, burst_id
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.listener_id,
        s.burst_id,
        s.session_start,
        s.session_end,
        s.event_count,
        s.duration_min,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.is_skipped AS play_is_skipped
    FROM per_session s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
play_track_id_stats AS (
    -- Median burst_id per play_track_id
    SELECT play_track_id, PERCENTILE_APPROX(burst_id, 0.5) AS median_burst_id
    FROM detail_rows_plays
    GROUP BY play_track_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_burst_id,
        ROUND(s.burst_id - g.median_burst_id, 2) AS burst_id_minus_median
    FROM detail_rows_plays s
    JOIN play_track_id_stats g ON g.play_track_id = s.play_track_id
    WHERE s.burst_id >= g.median_burst_id
)
-- Final projection
SELECT
    listener_id,
    burst_id,
    session_end,
    event_count,
    duration_min,
    play_id,
    play_track_id,
    play_is_skipped,
    median_burst_id,
    burst_id_minus_median,
    COALESCE(median_burst_id, 0.0) AS median_burst_id_filled
FROM vs_group
