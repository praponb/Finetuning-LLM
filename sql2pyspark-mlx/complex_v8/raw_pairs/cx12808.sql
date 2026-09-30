WITH filtered_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE major NOT IN ('Art', 'Bio')
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        is_international,
        DATE_TRUNC('WEEK', enrolled_on) AS enrolled_on_week,
        SUBSTRING(TRIM(student_name), 1, 2) AS student_name_prefix,
        DATE_FORMAT(enrolled_on, 'yyyy-MM') AS enrolled_on_label
    FROM filtered_students
    WHERE YEAR(enrolled_on) = 2024
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.cohort,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.grade_status AS enrollment_grade_status
    FROM labeled s
    JOIN enrollments e ON e.student_id = s.id
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.cohort,
        s.enrollment_id,
        s.enrollment_term,
        e.id AS enrollment_id_2,
        e.term AS enrollment_term_2,
        e.grade_status AS enrollment_grade_status_2
    FROM exploded_detail_enrollments s
    LEFT JOIN enrollments e ON e.student_id = s.id
)
-- Final projection
SELECT
    id,
    cohort,
    enrollment_id,
    enrollment_term,
    enrollment_id_2,
    enrollment_term_2,
    enrollment_grade_status_2
FROM detail_rows
WHERE enrollment_term_2 = '2024T2'
ORDER BY cohort DESC
