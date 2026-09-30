WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        credits,
        level
    FROM courses
    WHERE LOWER(course_code) LIKE '%35%'
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.level,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.score AS enrollment_score
    FROM prepared s
    LEFT JOIN enrollments e ON e.course_id = s.id AND e.enrolled_at >= TIMESTAMP '2025-02-06 11:00:00'
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        enrollment_id,
        enrollment_enrolled_at,
        enrollment_score,
        LOWER(REGEXP_REPLACE(TRIM(enrollment_grade_status), ' ', '_')) AS enrollment_grade_status_slug,
        DAYOFWEEK(enrollment_enrolled_at) AS enrollment_enrolled_at_dayofweek,
        COALESCE(enrollment_score, 0.0) AS enrollment_score_filled
    FROM detail_rows_enrollments
),
stats AS (
    -- Summarise per group
    SELECT
        enrollment_enrolled_at_dayofweek,
        YEAR(enrollment_enrolled_at) AS enrollment_enrolled_at_year,
        enrollment_id,
        MAX(enrollment_score) AS max_enrollment_score,
        COUNT(*) AS row_count
    FROM annotated
    WHERE enrollment_score_filled <= 85.0
    GROUP BY enrollment_enrolled_at_dayofweek, YEAR(enrollment_enrolled_at), enrollment_id
),
enriched_enrollment AS (
    -- Attach enrollment attributes
    SELECT
        s.enrollment_enrolled_at_dayofweek,
        s.enrollment_enrolled_at_year,
        s.enrollment_id,
        s.max_enrollment_score,
        s.row_count,
        e.course_id AS enrollment_course_id
    FROM stats s
    JOIN enrollments e ON e.id = s.enrollment_id AND e.grade_status <> 'PASS'
)
-- Final projection
SELECT
    enrollment_enrolled_at_dayofweek,
    enrollment_enrolled_at_year,
    max_enrollment_score,
    row_count
FROM enriched_enrollment
