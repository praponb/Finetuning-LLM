WITH filtered AS (
    -- Filter enrollments
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
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR (UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(prev_enrolled_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        course_id,
        session_no,
        MIN(enrolled_at) AS session_start,
        MAX(enrolled_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(enrolled_at)) - UNIX_TIMESTAMP(MIN(enrolled_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY course_id, session_no
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.course_id,
        s.session_no,
        s.duration_min,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.student_id AS enrollment_student_id
    FROM sessions s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
combined_details_course AS (
    -- Attach course attributes
    SELECT
        s.course_id,
        s.session_no,
        s.duration_min,
        s.enrollment_id,
        s.enrollment_score,
        s.enrollment_student_id,
        c.credits AS course_credits,
        c.level AS course_level,
        c.dept AS course_dept
    FROM with_children_enrollments s
    JOIN courses c ON c.id = s.course_id AND c.credits BETWEEN 2 AND 3
)
SELECT course_id, session_no, field, value
FROM (
    SELECT course_id, session_no, enrollment_score, duration_min
    FROM combined_details_course
) src
UNPIVOT (
    value FOR field IN (enrollment_score, duration_min)
) unp
