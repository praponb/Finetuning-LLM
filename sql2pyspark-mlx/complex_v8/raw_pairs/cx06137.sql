WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 2)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM valid s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM summarized_rows
    WHERE score > (SELECT MIN(score) FROM summarized_rows)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    enrollment_count
FROM without_matches
