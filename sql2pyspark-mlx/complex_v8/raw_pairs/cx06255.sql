WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
),
with_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        c.credits AS course_credits,
        c.course_code,
        c.dept AS course_dept
    FROM base_enrollments s
    JOIN courses c ON c.id = s.course_id
    WHERE UPPER(TRIM(c.course_code)) LIKE 'C%'
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    course_credits,
    course_dept,
    COALESCE(course_credits, 0) AS course_credits_filled
FROM with_details_course
ORDER BY course_credits_filled DESC, id
LIMIT 10
