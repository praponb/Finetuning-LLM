WITH scoped_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'C%'
),
scored AS (
    -- Derive helper columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(cohort), ' ', '_')) AS cohort_slug
    FROM scoped_students
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM scored
    WHERE gpa > (SELECT MIN(gpa) FROM scored)
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.cohort,
        s.enrolled_on,
        s.gpa,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM filtered s
    LEFT JOIN enrollments e ON e.student_id = s.id
)
-- Final projection
SELECT
    id,
    enrolled_on,
    enrollment_id,
    enrollment_score,
    enrollment_grade_status,
    enrollment_enrolled_at
FROM line_level
ORDER BY enrolled_on, id, enrollment_id
LIMIT 5
