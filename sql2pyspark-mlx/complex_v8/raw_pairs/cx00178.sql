WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE ms_played BETWEEN 95960 AND 360286
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    source,
    prev_played_at,
    starts_session,
    burst_id
FROM sessionized
ORDER BY starts_session
