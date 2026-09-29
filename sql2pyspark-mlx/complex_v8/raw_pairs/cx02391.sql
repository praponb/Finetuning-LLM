WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE score <= 78.8
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        student_id,
        ROUND(100 * COUNT(CASE WHEN term = '2024T2' THEN 1 END) / COUNT(*), 1) AS pct_term_2024t2,
        SUM(days_since_enrolled_at) AS total_days_since_enrolled_at,
        ROUND(AVG(days_since_enrolled_at), 2) AS avg_days_since_enrolled_at,
        ROUND(STDDEV(score), 2) AS stddev_score
    FROM scoped
    GROUP BY DATE_TRUNC('MONTH', enrolled_at), student_id
    HAVING MAX(score) > 5.7
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM summary s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score < 62.5
              AND e.grade_status IS NOT NULL
        )
)
-- Final projection
SELECT
    enrolled_at_month,
    student_id,
    total_days_since_enrolled_at,
    stddev_score
FROM without_matches
ORDER BY stddev_score DESC, total_days_since_enrolled_at DESC
