WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device
    FROM plays
    WHERE listener_id IS NOT NULL
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        track_id,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, session_id
),
joined_track AS (
    -- Enrich with dimension data
    SELECT
        s.track_id,
        s.session_id,
        s.session_start,
        s.session_end,
        s.n_events,
        s.session_ms_played,
        s.duration_min,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec
    FROM per_session s
    JOIN tracks t ON t.id = s.track_id AND ARRAY_CONTAINS(t.moods, 'chill')
)
SELECT
    track_id,
    session_end,
    n_events,
    session_ms_played,
    duration_min,
    track_duration_sec,
    CASE WHEN n_events >= 5 THEN 'HIGH' WHEN n_events >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS n_events_tier
FROM joined_track
