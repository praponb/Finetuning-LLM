WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
track_id_stats AS (
    -- Max is_new_session per track_id
    SELECT track_id, MAX(is_new_session) AS max_is_new_session
    FROM sessionized
    GROUP BY track_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_is_new_session,
        ROUND(s.is_new_session - g.max_is_new_session, 2) AS is_new_session_minus_max
    FROM sessionized s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.is_new_session = g.max_is_new_session
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    prev_played_at,
    burst_id,
    max_is_new_session,
    is_new_session_minus_max
FROM vs_group
ORDER BY ms_played
