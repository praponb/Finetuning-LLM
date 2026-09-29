WITH recent_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        enrolled_on,
        gpa,
        LAST_DAY(enrolled_on) AS enrolled_on_month_end
    FROM students
    WHERE gpa >= 2.0
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.enrolled_on,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status
    FROM recent_students s
    LEFT JOIN enrollments e ON e.student_id = s.id
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM exploded_detail_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.score >= 36.6
              AND e.term <> '2025T1'
        )
      AND s.enrollment_course_id <= 3
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        enrolled_on,
        enrollment_id,
        enrollment_grade_status,
        LAST_DAY(enrolled_on) AS enrolled_on_month_end,
        LOWER(REGEXP_REPLACE(TRIM(enrollment_grade_status), ' ', '_')) AS enrollment_grade_status_slug
    FROM screened
)
-- Final projection
SELECT
    id,
    enrollment_id,
    enrollment_grade_status,
    enrolled_on_month_end,
    enrollment_grade_status_slug,
    DATE_FORMAT(enrolled_on, 'yyyy-MM') AS enrolled_on_label,
    LOWER(TRIM(enrollment_grade_status)) AS enrollment_grade_status_clean
FROM enriched
WHERE YEAR(enrolled_on_month_end) = 2025
ORDER BY enrolled_on_label, id, enrollment_id
LIMIT 20
