WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM filtered_enrollments s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_at, grade_status, course_credits
    FROM combined_details
) src
PIVOT (
    MAX(course_credits)
    FOR grade_status IN ('INCOMPLETE', 'PASS', 'WITHDRAWN')
)
ORDER BY enrolled_at
