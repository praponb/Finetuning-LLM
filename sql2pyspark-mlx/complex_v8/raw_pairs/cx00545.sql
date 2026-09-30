WITH valid AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        is_international,
        DATE_TRUNC('YEAR', enrolled_on) AS enrolled_on_year,
        YEAR(enrolled_on) AS enrolled_on_year_2
    FROM students
    WHERE gpa >= 2.7
),
enriched AS (
    -- Derive helper columns
    SELECT *, MONTH(enrolled_on) AS enrolled_on_month
    FROM valid
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.course_id <= 5
        )
      AND s.major NOT IN ('Bio', 'Biz')
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.major,
        s.enrolled_on,
        s.enrolled_on_month,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score,
        e.term AS enrollment_term
    FROM with_activity s
    JOIN enrollments e ON e.student_id = s.id
)
SELECT
    id,
    student_name,
    major,
    enrolled_on,
    enrollment_id,
    enrollment_course_id,
    enrollment_score,
    enrollment_term,
    enrolled_on_month - enrollment_score AS enrolled_on_month_minus_enrollment_score
FROM line_level_enrollments
WHERE enrollment_id IS NOT NULL
ORDER BY enrolled_on_month_minus_enrollment_score
