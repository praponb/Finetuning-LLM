WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        CASE WHEN score >= 81.4 THEN 'large' WHEN score >= 26.2 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE score <= 57.6
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE term IN ('2025T1', '2025T2', '2024T1')) AS enrollment_term_count,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_term_count, 0) AS enrollment_term_count,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM recent_enrollments s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
SELECT id, metric, value
FROM (
    SELECT id, days_since_enrolled_at, enrollment_count
    FROM with_counts
) src
UNPIVOT (
    value FOR metric IN (days_since_enrolled_at, enrollment_count)
) unp
