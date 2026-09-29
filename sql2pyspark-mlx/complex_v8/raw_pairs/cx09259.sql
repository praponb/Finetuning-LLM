WITH filtered_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        level,
        LOWER(REGEXP_REPLACE(TRIM(level), ' ', '_')) AS level_slug
    FROM courses
    WHERE LENGTH(TRIM(course_code)) >= 8
),
enriched AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(level)) AS level_len
    FROM filtered_courses
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.level,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at
    FROM enriched s
    LEFT JOIN enrollments e ON e.course_id = s.id AND MONTH(e.enrolled_at) IN (1, 2)
)
-- Final projection
SELECT
    id,
    course_code,
    level,
    enrollment_id,
    enrollment_term,
    enrollment_score,
    enrollment_enrolled_at
FROM detail_rows
ORDER BY enrollment_score NULLS LAST, id, enrollment_id
LIMIT 20
