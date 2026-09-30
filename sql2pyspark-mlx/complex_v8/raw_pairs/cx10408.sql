WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
),
with_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM filtered_enrollments s
    JOIN courses c ON c.id = s.course_id AND c.credits <= 3
)
SELECT *
FROM (
    SELECT grade_status, enrolled_at, course_dept, course_credits
    FROM with_details_course
) src
PIVOT (
    SUM(course_credits)
    FOR course_dept IN ('Bio', 'Biz')
)
