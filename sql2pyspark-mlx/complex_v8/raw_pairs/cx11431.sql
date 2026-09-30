WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
    WHERE listener_id <= 4
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM base_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        source,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, burst_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM sessions
    WHERE burst_id > (SELECT AVG(burst_id) FROM sessions)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE burst_id > (SELECT AVG(burst_id) FROM without_matches)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT burst_id, source, n_events
    FROM with_activity
) src
PIVOT (
    COUNT(n_events)
    FOR source IN ('playlist', 'radio', 'search')
)
ORDER BY burst_id
