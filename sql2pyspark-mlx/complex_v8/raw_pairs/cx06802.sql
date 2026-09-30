WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term = '2025T1'
      AND course_id IS NOT NULL
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE term = '2024T2') AS enrollment_term_count
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count
    FROM filtered s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
with_details_student AS (
    -- Join students
    SELECT s.*, st.student_name
    FROM with_counts s
    JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    enrollment_term_count,
    student_name
FROM with_details_student
ORDER BY id DESC
