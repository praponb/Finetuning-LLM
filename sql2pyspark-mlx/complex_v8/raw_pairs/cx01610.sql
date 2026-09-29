WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        CASE WHEN source IN ('album', 'radio') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE track_id <= 28
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        source,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, session_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(duration_min - AVG(duration_min) OVER w_source_2, 2) AS duration_min_vs_source_avg,
        ROUND(AVG(session_id) OVER (PARTITION BY session_id ORDER BY session_end DESC, source, session_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS session_id_avg_6_prior,
        FIRST_VALUE(duration_min) IGNORE NULLS OVER (ORDER BY events DESC, source, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_min,
        ROUND(PERCENT_RANK() OVER (ORDER BY events DESC), 3) AS events_percent_rank,
        LEAD(session_end) OVER (ORDER BY events DESC, source, session_id) AS next_session_end,
        ROUND(AVG(events) OVER (ORDER BY events DESC, source, session_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS events_avg_4
    FROM session_summary
    WINDOW w_source_2 AS (PARTITION BY source)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE first_known_duration_min > (SELECT AVG(first_known_duration_min) FROM metrics)
)
SELECT
    source,
    session_id,
    session_end,
    duration_min,
    duration_min_vs_source_avg,
    session_id_avg_6_prior,
    first_known_duration_min,
    events_percent_rank,
    events_avg_4
FROM with_activity
