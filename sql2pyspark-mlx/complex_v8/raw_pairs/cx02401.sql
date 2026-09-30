WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.1, 2) AS score_adj
    FROM enrollments
    WHERE score > 6.2
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM recent_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        grade_status,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, burst_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(duration_min) OVER w_session_end_month AS session_end_month_min_duration_min,
        ROW_NUMBER() OVER w_event_count_by_session_end AS session_end_seq
    FROM sessions
    WINDOW w_session_end_month AS (PARTITION BY DATE_TRUNC('MONTH', session_end)), w_event_count_by_session_end AS (PARTITION BY event_count ORDER BY session_end DESC, grade_status, burst_id)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE burst_id > (SELECT AVG(burst_id) FROM metrics)
)
SELECT
    grade_status,
    session_end,
    event_count,
    duration_min,
    session_end_month_min_duration_min,
    session_end_seq,
    ROUND(duration_min * 1.07, 2) AS duration_min_adj
FROM screened
ORDER BY event_count NULLS LAST, session_end_month_min_duration_min
