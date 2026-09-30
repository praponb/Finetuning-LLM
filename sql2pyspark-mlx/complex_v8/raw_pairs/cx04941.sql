WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour
    FROM enrollments
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM base_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        prev_enrolled_at,
        is_new_session,
        session_id,
        SUM(session_id) OVER (ORDER BY is_new_session DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS session_id_sum_2,
        MAX(session_id) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', enrolled_at)) AS grade_status_enrolled_at_month_max_session_id
    FROM sessionized
),
enriched AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.prev_enrolled_at,
        s.is_new_session,
        s.session_id,
        s.session_id_sum_2,
        s.grade_status_enrolled_at_month_max_session_id,
        c.dept AS course_dept,
        st.gpa AS student_gpa
    FROM ranked s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    prev_enrolled_at,
    is_new_session,
    session_id,
    session_id_sum_2,
    grade_status_enrolled_at_month_max_session_id,
    course_dept,
    student_gpa
FROM enriched
ORDER BY course_id DESC, id NULLS LAST
