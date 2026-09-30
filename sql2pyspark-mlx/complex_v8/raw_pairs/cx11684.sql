WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        grade_status,
        session_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, session_id
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM session_summary
    WHERE event_count < (SELECT MAX(event_count) FROM session_summary)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(session_end, 3) OVER (ORDER BY duration_min, grade_status, session_id) AS next3_session_end,
        MAX(duration_min) OVER (PARTITION BY grade_status, MONTH(session_end)) AS grade_status_session_end_month_max_duration_min,
        MIN(event_count) OVER w_grade_status_session_end_mon AS grade_status_session_end_month_min_event_count,
        ROUND(session_id - AVG(session_id) OVER (PARTITION BY grade_status, MONTH(session_end)), 2) AS session_id_vs_grade_status_session_end_month_avg
    FROM with_activity
    WINDOW w_grade_status_session_end_mon AS (PARTITION BY grade_status, MONTH(session_end))
)
-- Final projection
SELECT
    grade_status,
    session_id,
    session_start,
    session_end,
    event_count,
    duration_min,
    next3_session_end,
    grade_status_session_end_month_min_event_count,
    CASE WHEN event_count >= 4 THEN 'HIGH' WHEN event_count >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS event_count_tier,
    CASE WHEN session_id >= 4 THEN 'A' WHEN session_id >= 2 THEN 'B' ELSE 'C' END AS session_id_tier
FROM sequenced
ORDER BY grade_status DESC
