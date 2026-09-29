WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
    WHERE grade_status IS NOT NULL
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) AS enrollment_count,
        ROUND(AVG(score), 2) AS avg_enrollment_score,
        COUNT(*) AS enrollment_count_2
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.avg_enrollment_score,
        COALESCE(e.enrollment_count_2, 0) AS enrollment_count_2
    FROM base_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score < 71.0
              AND e.grade_status NOT IN ('FAIL', 'PASS')
        )
      AND s.enrollment_count BETWEEN 2 AND 6
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score < 62.5
        )
      AND s.enrolled_at >= TIMESTAMP '2025-01-25 14:00:00'
)
-- Subtotals with ROLLUP
SELECT
    course_id,
    enrollment_count_2,
    term,
    COUNT(*) AS row_count,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list,
    COUNT(DISTINCT course_id) AS distinct_course_ids
FROM screened
GROUP BY ROLLUP(course_id, enrollment_count_2, term)
ORDER BY course_id NULLS LAST, enrollment_count_2 NULLS LAST, term NULLS LAST
