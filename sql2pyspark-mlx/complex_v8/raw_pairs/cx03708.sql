WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        dept,
        credits,
        level,
        LENGTH(TRIM(level)) AS level_len,
        CONCAT_WS(' / ', level, course_code) AS level_course_code_label
    FROM courses
    WHERE LOWER(course_code) LIKE '%cs%'
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.level,
        s.level_course_code_label,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM base s
    JOIN enrollments e ON e.course_id = s.id AND e.term <> '2024T2'
)
SELECT
    id,
    level_course_code_label,
    enrollment_id,
    enrollment_term,
    enrollment_grade_status,
    CONCAT_WS('-', level, enrollment_grade_status) AS level_enrollment_grade_status_label
FROM exploded_detail
ORDER BY level_enrollment_grade_status_label, id DESC
