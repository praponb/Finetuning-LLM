WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM valid_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        source,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, session_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        source,
        session_id,
        session_end,
        n_events,
        duration_min,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(session_end), source ORDER BY session_start ASC), 3) AS session_start_cume_dist,
        ROUND(session_id / NULLIF(SUM(session_id) OVER (PARTITION BY session_id), 0), 4) AS session_id_share_of_session_id,
        COUNT(*) OVER w_session_id AS rows_in_session_id,
        LAG(duration_min) OVER w_session_end_month_source_by_2 AS prev_duration_min
    FROM session_summary
    WINDOW w_session_id AS (PARTITION BY session_id), w_session_end_month_source_by_2 AS (PARTITION BY MONTH(session_end), source ORDER BY session_start ASC, source, session_id)
)
SELECT
    source,
    session_id,
    session_end,
    n_events,
    duration_min,
    session_start_cume_dist,
    session_id_share_of_session_id,
    rows_in_session_id,
    prev_duration_min,
    SUBSTRING(TRIM(source), 1, 2) AS source_prefix,
    COALESCE(duration_min, 0.0) AS duration_min_filled
FROM with_windows
WHERE source IS NOT NULL
ORDER BY prev_duration_min, source, session_id
LIMIT 10
