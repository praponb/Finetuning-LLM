WITH recent_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        enrolled_on,
        gpa,
        is_international
    FROM students
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM recent_students
    WHERE gpa > (SELECT MIN(gpa) FROM recent_students)
),
labeled AS (
    -- Label and score rows
    SELECT *, DATE_FORMAT(enrolled_on, 'yyyy-MM-dd') AS enrolled_on_label
    FROM screened
    WHERE enrolled_on BETWEEN DATE '2024-04-05' AND DATE '2025-05-07'
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.enrolled_on,
        s.enrolled_on_label,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id
    FROM labeled s
    JOIN enrollments e ON e.student_id = s.id
),
joined_enrollment AS (
    -- Attach enrollment attributes
    SELECT
        s.id,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        s.enrollment_course_id,
        e.student_id AS enrollment_student_id
    FROM exploded_detail_enrollments s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.enrollment_id,
        s.enrollment_enrolled_at,
        s.enrollment_course_id,
        s.enrollment_student_id,
        c.level AS course_level,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM joined_enrollment s
    LEFT JOIN courses c ON c.id = s.enrollment_course_id
)
SELECT
    id,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_course_id,
    enrollment_student_id,
    course_level,
    course_dept
FROM with_parent
ORDER BY enrollment_id
