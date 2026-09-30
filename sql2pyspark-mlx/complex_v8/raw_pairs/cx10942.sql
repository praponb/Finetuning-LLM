WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        source,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, burst_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        source,
        burst_id,
        session_end,
        session_ms_played,
        duration_min,
        SUM(session_ms_played) OVER (PARTITION BY burst_id, DATE_TRUNC('MONTH', session_end)) AS burst_id_session_end_month_sum_session_ms_played,
        SUM(duration_min) OVER (PARTITION BY source, event_count ORDER BY duration_min NULLS LAST, source, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_min,
        ROUND(PERCENT_RANK() OVER w_session_end_month_by_duratio, 3) AS duration_min_percent_rank
    FROM session_summary
    WINDOW w_session_end_month_by_duratio AS (PARTITION BY DATE_TRUNC('MONTH', session_end) ORDER BY duration_min DESC)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE running_duration_min > (SELECT AVG(running_duration_min) FROM with_trends)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT burst_id, session_end, source, session_ms_played
    FROM filtered
) src
PIVOT (
    MAX(session_ms_played)
    FOR source IN ('radio', 'search', 'album')
)
