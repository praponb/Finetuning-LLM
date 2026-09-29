WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label
    FROM enrollments
    WHERE grade_status = 'PASS'
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND TO_DATE(e.enrolled_at) < DATE '2025-03-02'
        )
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_label,
        c.level AS course_level,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM without_matches s
    JOIN courses c ON c.id = s.course_id
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.course_credits,
        s.course_dept,
        c.dept AS course_dept_2
    FROM joined_course s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    course_id,
    enrolled_at,
    course_credits,
    course_dept
FROM with_parent_course
WHERE course_credits < 4
ORDER BY course_credits, id
LIMIT 20
