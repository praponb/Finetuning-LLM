WITH base_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        major,
        is_international,
        COALESCE(gpa, 0.0) AS gpa_filled,
        CASE WHEN cohort IN ('C2023', 'C2022') THEN 'group_a' WHEN cohort = 'C2024' THEN 'group_b' ELSE 'other' END AS cohort_group
    FROM students
    WHERE enrolled_on BETWEEN DATE '2024-01-24' AND DATE '2025-06-27'
      AND cohort NOT IN ('C2022', 'C2023')
),
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.is_international,
        s.gpa_filled,
        s.cohort_group,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM base_students s
    JOIN enrollments e ON e.student_id = s.id AND e.grade_status NOT IN ('WITHDRAWN', 'PASS')
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.is_international,
        s.gpa_filled,
        s.cohort_group,
        s.enrollment_id,
        s.enrollment_course_id,
        s.enrollment_enrolled_at,
        s.enrollment_term,
        s.enrollment_grade_status,
        e.id AS enrollment_id_2,
        e.course_id AS enrollment_course_id_2,
        e.term AS enrollment_term_2,
        e.grade_status AS enrollment_grade_status_2
    FROM detail_rows_enrollments s
    JOIN enrollments e ON e.student_id = s.id
),
annotated AS (
    -- Label and score rows
    SELECT *, COALESCE(enrollment_term, 'UNKNOWN') AS enrollment_term_or_unknown
    FROM exploded_detail
)
-- Final projection
SELECT
    id,
    is_international,
    gpa_filled,
    cohort_group,
    enrollment_id,
    enrollment_course_id,
    enrollment_grade_status,
    enrollment_term_2,
    enrollment_grade_status_2,
    enrollment_term_or_unknown
FROM annotated
WHERE enrollment_course_id IS NOT NULL
ORDER BY id DESC NULLS FIRST
