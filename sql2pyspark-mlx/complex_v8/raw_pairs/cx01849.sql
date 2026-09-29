WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE course_id <= 7
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) AS enrollment_count,
        COUNT(*) FILTER (WHERE grade_status = 'FAIL') AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM filtered s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, grade_status_len, enrollment_count
    FROM with_counts
) src
UNPIVOT (
    value FOR measure IN (grade_status_len, enrollment_count)
) unp
