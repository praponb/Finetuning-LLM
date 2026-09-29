WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE score > 38.5
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at_label,
        s.grade_status_slug,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.term AS enrollment_term
    FROM prepared_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count
    FROM detail_rows_enrollments s
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_slug,
        s.enrollment_id,
        s.first_enrollment_enrolled_at,
        s.enrollment_count,
        e.id AS enrollment_id_2,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id
    FROM with_stats s
    JOIN enrollments e ON e.student_id = s.student_id
),
enrollment_stats_2 AS (
    -- Per-course enrollments metrics
    SELECT course_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM with_children_enrollments s
    LEFT JOIN enrollment_stats_2 e ON e.course_id = s.course_id
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_slug,
        s.enrollment_id,
        s.first_enrollment_enrolled_at,
        s.enrollment_id_2,
        s.enrollment_enrolled_at,
        s.enrollment_course_id,
        s.last_enrollment_enrolled_at,
        e.id AS enrollment_id_3,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status
    FROM with_counts s
    JOIN enrollments e ON e.student_id = s.student_id AND e.grade_status IN ('INCOMPLETE', 'WITHDRAWN')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status_slug,
        s.enrollment_id,
        s.first_enrollment_enrolled_at,
        s.enrollment_id_2,
        s.enrollment_enrolled_at,
        s.enrollment_course_id,
        s.last_enrollment_enrolled_at,
        s.enrollment_id_3,
        s.enrollment_grade_status,
        e.grade_status AS enrollment_grade_status_2,
        e.term AS enrollment_term,
        e.student_id AS enrollment_student_id
    FROM line_level s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
)
SELECT
    id,
    student_id,
    grade_status_slug,
    enrollment_id,
    enrollment_id_2,
    enrollment_enrolled_at,
    enrollment_course_id,
    last_enrollment_enrolled_at,
    enrollment_id_3,
    enrollment_grade_status,
    enrollment_grade_status_2,
    enrollment_term,
    TO_DATE(enrollment_enrolled_at) AS enrollment_enrolled_at_date,
    CONCAT_WS(': ', grade_status_slug, enrollment_grade_status) AS grade_status_slug_enrollment_grade_status_label
FROM with_parent
WHERE enrollment_id <= 82
ORDER BY grade_status_slug DESC, enrollment_enrolled_at, id, enrollment_id, enrollment_id_2, enrollment_id_3
LIMIT 5
