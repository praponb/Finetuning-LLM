WITH filtered AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
combined_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        s.prev_played_at,
        s.new_session_flag,
        s.session_no,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username
    FROM sessionized s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.birth_year > 1967
)
SELECT
    id,
    played_at,
    device,
    new_session_flag,
    listener_interests,
    listener_birth_year,
    listener_username
FROM combined_details_listener
WHERE played_at >= TIMESTAMP '2025-01-19 11:00:00'
ORDER BY id
