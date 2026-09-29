WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        grade_status,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, session_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(session_score) OVER (PARTITION BY TO_DATE(session_end), event_count ORDER BY duration_min, grade_status, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_session_score,
        ROUND(event_count / NULLIF(SUM(event_count) OVER w_event_count, 0), 4) AS event_count_share_of_event_count,
        COUNT(*) OVER w_event_count AS rows_in_event_count,
        LEAD(session_score, 3) OVER w_event_count_by_session_end AS next3_session_score,
        LAST_VALUE(event_count) IGNORE NULLS OVER (PARTITION BY event_count ORDER BY session_end DESC, grade_status, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_count
    FROM sessions
    WINDOW w_event_count AS (PARTITION BY event_count), w_event_count_by_session_end AS (PARTITION BY event_count ORDER BY session_end DESC, grade_status, session_id)
),
grouped AS (
    -- Group-level totals
    SELECT
        last_known_event_count,
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        MAX(session_start) AS last_session_start,
        ROUND(SUM(last_session_score) / NULLIF(SUM(event_count_share_of_event_count), 0), 3) AS last_session_score_to_event_count_share_of_event,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(event_count_share_of_event_count), 2) AS stddev_event_count_share_of_event_count
    FROM windowed
    WHERE MONTH(session_end) IN (1, 3)
    GROUP BY 1, 2
)
SELECT
    last_known_event_count,
    session_end_month,
    last_session_start,
    last_session_score_to_event_count_share_of_event,
    n_rows,
    stddev_event_count_share_of_event_count,
    CASE WHEN n_rows >= 10 THEN 'large' WHEN n_rows >= 4 THEN 'medium' ELSE 'small' END AS n_rows_tier
FROM grouped
ORDER BY last_session_score_to_event_count_share_of_event, stddev_event_count_share_of_event_count NULLS LAST, last_known_event_count, session_end_month
LIMIT 20
