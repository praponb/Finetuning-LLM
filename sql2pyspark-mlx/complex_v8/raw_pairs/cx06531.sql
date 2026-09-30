WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE source IS NOT NULL
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered_plays
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
    ms_played,
    device,
    is_skipped,
    source,
    starts_session,
    burst_id,
    ROUND(ms_played / NULLIF(starts_session, 0), 3) AS ms_played_per_starts_session,
    ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60, 1) AS minutes_prev_played_at_to_played_at
FROM sessionized
WHERE device IS NOT NULL
ORDER BY starts_session DESC NULLS FIRST, id
LIMIT 20
