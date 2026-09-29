WITH prepared AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_parent AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.prev_played_at,
        s.starts_session,
        s.session_id,
        t.is_explicit AS track_is_explicit,
        t.title AS track_title
    FROM sessionized s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.is_explicit = TRUE
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    prev_played_at,
    starts_session,
    ROUND(starts_session / NULLIF(session_id, 0), 3) AS starts_session_per_session_id,
    DATEDIFF(DATE '2025-12-31', prev_played_at) AS days_since_prev_played_at
FROM with_parent
ORDER BY days_since_prev_played_at, track_id NULLS LAST
