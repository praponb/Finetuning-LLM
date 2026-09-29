WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE term = '2024T1'
),
with_prev AS (
    -- Previous enrolled_at per student_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        student_id,
        session_no,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(score) AS session_score,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY student_id, session_no
),
enriched AS (
    -- Join students
    SELECT
        s.student_id,
        s.session_no,
        s.session_score,
        s.duration_min,
        st.enrolled_on AS student_enrolled_on
    FROM per_session s
    JOIN students st ON st.id = s.student_id
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.student_id,
        s.session_no,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM enriched s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
)
SELECT
    student_id,
    session_no,
    enrollment_id,
    enrollment_course_id,
    enrollment_enrolled_at
FROM detail_rows
ORDER BY enrollment_id NULLS LAST, student_id, session_no
LIMIT 20
