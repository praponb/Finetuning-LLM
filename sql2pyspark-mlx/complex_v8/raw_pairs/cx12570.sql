WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        st.is_international AS student_is_international,
        st.student_name
    FROM valid_enrollments s
    JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at,
    student_is_international,
    student_name,
    DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
FROM with_details
ORDER BY enrolled_at_label, student_name, id
LIMIT 20
