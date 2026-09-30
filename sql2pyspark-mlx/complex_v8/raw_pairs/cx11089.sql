WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
    WHERE term IN ('2025T2', '2024T1', '2024T2')
),
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        st.enrolled_on AS student_enrolled_on
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    grade_status,
    enrolled_at,
    student_enrolled_on,
    TO_DATE(enrolled_at) AS enrolled_at_date
FROM joined_student
ORDER BY enrolled_at, id DESC
