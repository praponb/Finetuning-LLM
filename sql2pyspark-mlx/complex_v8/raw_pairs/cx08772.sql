WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CONCAT_WS('-', source, device) AS source_device_label
    FROM plays
    WHERE is_skipped = TRUE
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        listener_id,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, session_no
),
enriched_listener AS (
    -- Enrich with dimension data
    SELECT
        s.listener_id,
        s.session_no,
        s.session_start,
        s.session_end,
        s.events,
        s.duration_min,
        l.interests AS listener_interests,
        l.username AS listener_username,
        l.birth_year AS listener_birth_year
    FROM sessions s
    JOIN listeners l ON l.id = s.listener_id
)
-- Final projection
SELECT
    listener_id,
    session_no,
    session_end,
    events,
    duration_min,
    CASE WHEN duration_min >= 168.0 THEN 'large' WHEN duration_min >= 48.0 THEN 'medium' ELSE 'small' END AS duration_min_tier,
    ROUND(duration_min * 1.5, 2) AS duration_min_adj
FROM enriched_listener
