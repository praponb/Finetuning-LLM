WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 241585 THEN 'A' WHEN ms_played >= 107912 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM plays
    WHERE ms_played >= 98052
),
with_prev AS (
    -- Previous played_at per ms_played_tier
    SELECT *, LAG(played_at) OVER (PARTITION BY ms_played_tier ORDER BY played_at, id) AS prev_played_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY ms_played_tier ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        ms_played_tier,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ms_played_tier, session_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        ms_played_tier,
        session_id,
        session_start,
        events,
        session_ms_played,
        duration_min,
        ROUND(AVG(duration_min) OVER (ORDER BY session_end DESC, ms_played_tier, session_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS duration_min_avg_2_prior,
        MIN(events) OVER (ORDER BY session_end DESC, ms_played_tier, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_events_so_far,
        ROUND(session_id / NULLIF(SUM(session_id) OVER (PARTITION BY DATE_TRUNC('MONTH', session_start)), 0), 4) AS session_id_share_of_session_start_month
    FROM per_session
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE min_events_so_far < (SELECT MAX(min_events_so_far) FROM ranked)
)
SELECT
    ms_played_tier,
    session_id,
    session_start,
    events,
    session_ms_played,
    duration_min,
    duration_min_avg_2_prior,
    min_events_so_far,
    session_id_share_of_session_start_month,
    CASE WHEN duration_min >= 204.0 THEN 'large' WHEN duration_min >= 48.0 THEN 'medium' ELSE 'small' END AS duration_min_tier,
    COALESCE(ms_played_tier, 'UNKNOWN') AS ms_played_tier_or_unknown
FROM without_matches
ORDER BY session_id, ms_played_tier
LIMIT 20
