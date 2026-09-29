WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND term IS NOT NULL
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM recent_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status IS NOT NULL
        )
      AND s.enrolled_at < TIMESTAMP '2025-03-08 09:00:00'
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    grade_status_slug,
    INITCAP(TRIM(grade_status_slug)) AS grade_status_slug_clean
FROM qualified
WHERE grade_status NOT IN ('PASS', 'INCOMPLETE')
