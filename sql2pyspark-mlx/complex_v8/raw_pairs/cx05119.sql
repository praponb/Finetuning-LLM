WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at_month per student_id
    SELECT *, LAG(enrolled_at_month) OVER (PARTITION BY student_id ORDER BY enrolled_at_month, id) AS prev_enrolled_at_month
    FROM scoped_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_enrolled_at_month IS NULL OR enrolled_at_month > prev_enrolled_at_month + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY student_id ORDER BY enrolled_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        student_id,
        session_no,
        MIN(enrolled_at_month) AS session_start,
        MAX(enrolled_at_month) AS session_end,
        COUNT(*) AS events,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at_month)) - UNIX_TIMESTAMP(MIN(enrolled_at_month))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, session_no
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.student_id,
        s.session_no,
        s.session_end,
        s.events,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score
    FROM session_summary s
    JOIN enrollments e ON e.student_id = s.student_id AND e.term <> '2025T2'
),
enriched AS (
    -- Derive helper columns
    SELECT
        student_id,
        session_no,
        session_end,
        enrollment_id,
        enrollment_course_id,
        GREATEST(enrollment_score, session_no) AS greatest_enrollment_score_session_no,
        CONCAT(enrollment_term, '#', CAST(session_no AS STRING)) AS enrollment_term_ref
    FROM detail_rows
)
SELECT
    student_id,
    session_no,
    session_end,
    enrollment_id,
    enrollment_course_id,
    greatest_enrollment_score_session_no,
    enrollment_term_ref,
    UPPER(TRIM(enrollment_term_ref)) AS enrollment_term_ref_clean,
    SUBSTRING(TRIM(enrollment_term_ref), 1, 4) AS enrollment_term_ref_prefix
FROM enriched
ORDER BY enrollment_term_ref_clean, greatest_enrollment_score_session_no DESC NULLS FIRST
