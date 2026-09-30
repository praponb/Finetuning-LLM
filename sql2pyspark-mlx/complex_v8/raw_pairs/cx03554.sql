WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE ms_played >= 160264
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM valid_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
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
tagged AS (
    -- Derive helper columns
    SELECT
        source,
        burst_id,
        session_end,
        n_events,
        n_events - burst_id AS n_events_minus_burst_id,
        TO_DATE(session_start) AS session_start_date
    FROM per_session
    WHERE source IN ('radio', 'search', 'playlist')
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE n_events > (SELECT AVG(n_events) FROM tagged)
)
-- Final projection
SELECT
    source,
    session_end,
    n_events,
    n_events_minus_burst_id,
    session_start_date
FROM qualified
WHERE session_start_date >= DATE '2024-05-01'
ORDER BY session_end NULLS LAST, n_events_minus_burst_id
