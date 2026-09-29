WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM valid_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    session_id,
    listener_id,
    track_id,
    MAX(played_at) AS last_played_at,
    MAX(session_id) AS max_session_id
FROM sessionized
GROUP BY CUBE(session_id, listener_id, track_id)
ORDER BY session_id NULLS LAST, listener_id NULLS LAST, track_id NULLS LAST
