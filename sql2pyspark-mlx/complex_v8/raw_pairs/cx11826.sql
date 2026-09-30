WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE score > 17.4
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.total_enrollment_score
    FROM recent_enrollments s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score < 83.1
              AND HOUR(e.enrolled_at) BETWEEN 8 AND 14
        )
      AND s.total_enrollment_score BETWEEN 150.0 AND 450.0
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    score,
    grade_status,
    enrolled_at,
    score_filled,
    grade_status_prefix,
    days_since_enrolled_at
FROM without_matches
WHERE days_since_enrolled_at >= 175
ORDER BY score NULLS LAST
