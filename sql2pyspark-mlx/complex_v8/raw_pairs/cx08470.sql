WITH cleaned AS (
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
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) FILTER (WHERE grade_status = 'PASS') AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM cleaned s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status <> 'INCOMPLETE'
        )
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term = '2025T1'
        )
)
-- Subtotals with GROUPING SETS
SELECT
    course_id,
    COALESCE(grade_status, 'ALL') AS grade_status,
    COUNT(*) AS row_count,
    MIN(enrolled_at) AS first_enrolled_at,
    COUNT(DISTINCT student_id) AS distinct_student_ids
FROM qualified
GROUP BY GROUPING SETS ((course_id, grade_status), (course_id), (grade_status))
