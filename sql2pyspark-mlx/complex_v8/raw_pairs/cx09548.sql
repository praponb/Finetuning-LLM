WITH base AS (
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
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.prev_enrolled_at,
        s.starts_session,
        s.session_no,
        st.major AS student_major,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa
    FROM sessionized s
    JOIN students st ON st.id = s.student_id
    WHERE st.gpa > 2.5
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN starts_session >= 1 THEN 'A' WHEN starts_session >= 0 THEN 'B' ELSE 'C' END AS starts_session_tier,
        ABS(starts_session - 0) AS starts_session_distance,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek
    FROM with_details_student
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    prev_enrolled_at,
    session_no,
    student_major,
    starts_session_distance,
    enrolled_at_dayofweek,
    DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
FROM scored
WHERE student_major = 'Bio'
ORDER BY prev_enrolled_at DESC, id
LIMIT 5
