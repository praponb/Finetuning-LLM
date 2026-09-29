WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        YEAR(played_at) AS played_at_year
    FROM plays
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        track_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, burst_id
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM per_session s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.listener_id >= 4
              AND p.device NOT IN ('phone', 'speaker')
        )
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(played_at) AS last_played_at,
        MAX(played_at) AS last_played_at_2,
        MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        p.last_played_at_2,
        p.first_played_at
    FROM qualified s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
)
-- Final projection
SELECT
    track_id,
    burst_id,
    session_start,
    session_end,
    events,
    duration_min,
    last_played_at,
    last_played_at_2,
    first_played_at,
    CASE WHEN events >= 5 THEN 'A' WHEN events >= 2 THEN 'B' ELSE 'C' END AS events_tier
FROM with_counts
