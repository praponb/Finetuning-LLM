WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        student_id,
        session_no,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, session_no
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.student_id,
        s.session_no,
        s.session_start,
        s.session_end,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score
    FROM session_summary s
    JOIN enrollments e ON e.student_id = s.student_id AND e.score BETWEEN 5.6 AND 76.8
),
summary AS (
    -- Summarise per group
    SELECT
        enrollment_id,
        MIN(session_start) AS first_session_start,
        ROUND(SUM(session_no) / NULLIF(SUM(enrollment_score), 0), 3) AS session_no_to_enrollment_score_ratio,
        ROUND(STDDEV(enrollment_score), 2) AS stddev_enrollment_score,
        MIN(session_no) AS min_session_no,
        SUM(session_no) AS total_session_no,
        MAX(session_end) AS last_session_end
    FROM line_level_enrollments
    GROUP BY 1
)
-- Final projection
SELECT
    enrollment_id,
    first_session_start,
    session_no_to_enrollment_score_ratio,
    stddev_enrollment_score,
    min_session_no,
    total_session_no,
    last_session_end,
    HOUR(last_session_end) AS last_session_end_hour
FROM summary
ORDER BY last_session_end DESC, enrollment_id
LIMIT 5
