WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
    WHERE source <> 'playlist'
),
with_prev AS (
    -- Previous played_at per is_skipped_label
    SELECT *, LAG(played_at) OVER (PARTITION BY is_skipped_label ORDER BY played_at, id) AS prev_played_at
    FROM filtered_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY is_skipped_label ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        is_skipped_label,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY is_skipped_label, burst_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        is_skipped_label,
        burst_id,
        duration_min,
        ROUND(AVG(event_count) OVER (ORDER BY event_count DESC, is_skipped_label, burst_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS event_count_avg_2_prior,
        COUNT(*) OVER w_is_skipped_label_session_end AS rows_in_is_skipped_label_session_end_year,
        LAG(session_start) OVER (PARTITION BY event_count, burst_id ORDER BY event_count DESC, is_skipped_label, burst_id) AS prev_session_start,
        ROUND(event_count / NULLIF(SUM(event_count) OVER (PARTITION BY is_skipped_label, YEAR(session_end)), 0), 4) AS event_count_share_of_is_skipped_label_session_en
    FROM per_session
    WINDOW w_is_skipped_label_session_end AS (PARTITION BY is_skipped_label, YEAR(session_end))
)
-- Subtotals with GROUPING SETS
SELECT
    burst_id,
    COALESCE(is_skipped_label, 'ALL') AS is_skipped_label,
    COUNT(DISTINCT is_skipped_label) AS distinct_is_skipped_labels,
    COUNT(*) AS row_count,
    MAX(burst_id) AS max_burst_id
FROM sequenced
GROUP BY GROUPING SETS ((burst_id, is_skipped_label), (burst_id))
