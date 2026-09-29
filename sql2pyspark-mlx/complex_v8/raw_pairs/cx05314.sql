WITH prepared AS (
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
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        source,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, session_no
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(source, '#', CAST(session_ms_played AS STRING)) AS source_ref,
        DATEDIFF(DATE '2025-12-31', session_end) AS days_since_session_end,
        ABS(n_events - 3) AS n_events_distance
    FROM session_summary
    WHERE MONTH(session_end) IN (2, 3)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(source_ref)) AS source_ref_len,
        DATE_FORMAT(session_end, 'MMM yyyy') AS session_end_label
    FROM annotated
    WHERE source_ref IS NOT NULL
)
SELECT
    source,
    session_no,
    session_start,
    session_end,
    n_events,
    duration_min,
    source_ref,
    FLOOR(source_ref_len / 2) * 2 AS source_ref_len_bucket,
    CONCAT_WS(' / ', source_ref, source) AS source_ref_source_label
FROM enriched
ORDER BY session_start DESC, source_ref_len_bucket DESC, source, session_no
LIMIT 20
