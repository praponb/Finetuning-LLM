WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ABS(ms_played - 176302) AS ms_played_distance
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        listener_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, burst_id
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT s.*, l.birth_year AS listener_birth_year
    FROM session_summary s
    JOIN listeners l ON l.id = s.listener_id
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.listener_id,
        s.burst_id,
        s.session_start,
        s.events,
        s.listener_birth_year,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped
    FROM with_parent_listener s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    events,
    listener_id,
    PERCENTILE_APPROX(listener_birth_year, 0.5) AS median_listener_birth_year,
    MAX(session_start) AS last_session_start,
    COUNT(DISTINCT listener_id) AS distinct_listener_ids
FROM line_level_plays
GROUP BY GROUPING SETS ((events, listener_id), (events), ())
ORDER BY events NULLS LAST, listener_id NULLS LAST
