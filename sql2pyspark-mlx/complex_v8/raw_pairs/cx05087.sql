WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM prepared_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.is_new_session,
        s.burst_id,
        st.enrolled_on AS student_enrolled_on,
        c.course_code
    FROM sessionized s
    JOIN students st ON st.id = s.student_id AND st.gpa >= 2.7
    JOIN courses c ON c.id = s.course_id
),
latest_per_key AS (
    -- Keep the latest row per term, course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        is_new_session,
        burst_id,
        student_enrolled_on,
        course_code
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, course_id ORDER BY enrolled_at DESC, id DESC) AS row_num
        FROM with_details
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    course_id,
    score,
    grade_status,
    is_new_session,
    student_enrolled_on,
    LEAST(burst_id, is_new_session) AS least_burst_id_is_new_session
FROM latest_per_key
WHERE is_new_session >= 0
