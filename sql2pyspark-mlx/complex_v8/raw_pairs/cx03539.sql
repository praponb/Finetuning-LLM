WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE device IN ('speaker', 'tv')
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM cleaned_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        source,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, session_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        source,
        session_id,
        session_start,
        session_ms_played,
        duration_min,
        ROW_NUMBER() OVER (ORDER BY event_count, source, session_id) AS event_count_seq,
        COUNT(*) OVER (PARTITION BY MONTH(session_start)) AS rows_in_session_start_month,
        session_ms_played - LAG(session_ms_played) OVER (PARTITION BY source, event_count ORDER BY session_end, source, session_id) AS session_ms_played_change,
        FIRST_VALUE(session_start) OVER (ORDER BY event_count, source, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_start
    FROM per_session
)
SELECT
    source,
    session_id,
    session_start,
    session_ms_played,
    duration_min,
    event_count_seq,
    session_ms_played_change,
    first_session_start,
    ROUND(event_count_seq / NULLIF(session_ms_played_change, 0), 3) AS event_count_seq_per_session_ms_played_change
FROM with_trends
ORDER BY session_ms_played DESC, source, session_id
LIMIT 10
