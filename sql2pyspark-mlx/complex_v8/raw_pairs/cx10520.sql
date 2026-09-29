WITH filtered AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        ABS(credits - 2) AS credits_distance
    FROM courses
    WHERE dept IN ('CS', 'Art')
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.dept,
        s.credits,
        s.level,
        s.credits_distance,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM filtered s
    LEFT JOIN enrollments e ON e.course_id = s.id AND e.term = '2025T1'
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_children_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.grade_status = 'WITHDRAWN'
        )
)
SELECT
    id,
    dept,
    credits,
    level,
    credits_distance,
    enrollment_id,
    enrollment_term,
    enrollment_enrolled_at,
    DATE_FORMAT(enrollment_enrolled_at, 'yyyy-MM-dd') AS enrollment_enrolled_at_label
FROM qualified
ORDER BY enrollment_term DESC NULLS FIRST, id, enrollment_id
LIMIT 5
