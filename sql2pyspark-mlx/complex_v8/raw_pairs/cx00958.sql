WITH valid AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
    WHERE score < 76.8
      AND HOUR(enrolled_at) BETWEEN 9 AND 12
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        MIN(score) AS min_enrollment_score,
        MIN(enrolled_at) AS first_enrollment_enrolled_at_2
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at,
        e.min_enrollment_score,
        e.first_enrollment_enrolled_at_2
    FROM valid s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
-- Subtotals with GROUPING SETS
SELECT
    student_id,
    grade_status,
    ROUND(SUM(score) / NULLIF(SUM(min_enrollment_score), 0), 3) AS score_to_min_enrollment_score_ratio,
    COUNT(*) AS row_count
FROM summarized_rows
GROUP BY GROUPING SETS ((student_id, grade_status), (student_id), (grade_status), ())
ORDER BY student_id NULLS LAST, grade_status NULLS LAST
