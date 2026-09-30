WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE course_id <= 6
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        term_grade_status_label,
        LOWER(REGEXP_REPLACE(TRIM(term_grade_status_label), ' ', '_')) AS term_grade_status_label_slug
    FROM valid_enrollments
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        e.id AS enrollment_id,
        e.student_id AS enrollment_student_id,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM tagged s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
)
SELECT
    id,
    course_id,
    enrollment_student_id,
    enrollment_term,
    SUBSTRING(TRIM(enrollment_grade_status), 1, 2) AS enrollment_grade_status_prefix
FROM with_children_enrollments
WHERE enrollment_id IS NOT NULL
ORDER BY course_id DESC
