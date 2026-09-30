WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per grade_status
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        grade_status,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY grade_status, burst_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(event_count) OVER w_burst_id_grade_status, 2) AS burst_id_grade_status_avg_event_count,
        NTH_VALUE(event_count, 2) OVER (PARTITION BY burst_id, MONTH(session_end) ORDER BY event_count, grade_status, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_count
    FROM per_session
    WINDOW w_burst_id_grade_status AS (PARTITION BY burst_id, grade_status)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE session_score > (SELECT AVG(session_score) FROM sequenced)
)
-- Final projection
SELECT
    grade_status,
    burst_id,
    session_start,
    session_end,
    event_count,
    session_score,
    duration_min,
    burst_id_grade_status_avg_event_count,
    second_event_count
FROM qualified
