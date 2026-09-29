WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        LOWER(TRIM(device)) AS device_clean
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM valid_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    is_skipped,
    source,
    device_clean,
    starts_session,
    session_id,
    DATE_FORMAT(prev_played_at, 'MMM yyyy') AS prev_played_at_label
FROM sessionized
ORDER BY starts_session DESC
