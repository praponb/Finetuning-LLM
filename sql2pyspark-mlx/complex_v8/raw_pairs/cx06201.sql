WITH base AS (
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
    WHERE is_skipped = FALSE
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.prev_played_at,
        s.starts_session,
        l.username AS listener_username,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year
    FROM sessionized s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.country IS NOT NULL
)
SELECT
    starts_session,
    track_id,
    source,
    GROUPING_ID() AS gid,
    COUNT(*) FILTER (WHERE device IN ('desktop', 'speaker', 'car')) AS n_device_in_desktop_speaker_ca
FROM with_parent_listener
GROUP BY GROUPING SETS ((starts_session, track_id, source), (starts_session))
ORDER BY starts_session NULLS LAST, track_id NULLS LAST, source NULLS LAST
