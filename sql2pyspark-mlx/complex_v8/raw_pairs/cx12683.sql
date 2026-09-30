WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE ms_played <= 261579
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM cleaned_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        track_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, burst_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('WEEK', session_end) AS session_end_week,
        CASE WHEN events >= 4 THEN 'HIGH' WHEN events >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS events_tier,
        events - duration_min AS events_minus_duration_min
    FROM session_summary
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS total_play_ms_played,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS max_play_ms_played
    FROM tagged s
)
SELECT
    track_id,
    burst_id,
    session_start,
    session_end,
    events,
    events_tier,
    events_minus_duration_min,
    total_play_ms_played,
    play_count,
    max_play_ms_played
FROM with_counts
ORDER BY track_id, events_tier
