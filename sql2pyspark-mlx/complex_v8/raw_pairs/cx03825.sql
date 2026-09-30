WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.level,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.student_id AS enrollment_student_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM recent s
    JOIN enrollments e ON e.course_id = s.id AND e.enrolled_at < TIMESTAMP '2025-03-08 07:00:00'
),
joined_enrollment AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.course_code,
        s.level,
        s.enrollment_id,
        s.enrollment_score,
        s.enrollment_student_id,
        e.score AS enrollment_score_2,
        st.student_name,
        st.is_international AS student_is_international
    FROM line_level s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN students st ON st.id = s.enrollment_student_id
),
with_parent AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.course_code,
        s.enrollment_id,
        s.enrollment_score,
        s.enrollment_student_id,
        s.student_is_international,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international_2
    FROM joined_enrollment s
    JOIN students st ON st.id = s.enrollment_student_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrollment_student_id, enrollment_id, student_cohort, enrollment_score
    FROM with_parent
) src
PIVOT (
    MAX(enrollment_score)
    FOR student_cohort IN ('C2022', 'C2024', 'C2021', 'C2023')
)
