WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        student_id,
        burst_id,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, burst_id
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.student_id,
        s.burst_id,
        s.session_end,
        s.duration_min,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score
    FROM sessions s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
)
-- Final projection
SELECT
    student_id,
    burst_id,
    duration_min,
    enrollment_id,
    enrollment_term,
    enrollment_enrolled_at,
    enrollment_course_id,
    enrollment_score,
    CONCAT(enrollment_term, '#', CAST(enrollment_course_id AS STRING)) AS enrollment_term_ref,
    burst_id - duration_min AS burst_id_minus_duration_min
FROM detail_rows
ORDER BY enrollment_score DESC
