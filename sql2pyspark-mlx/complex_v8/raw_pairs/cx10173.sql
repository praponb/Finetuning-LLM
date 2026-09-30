WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        CASE WHEN term IN ('2024T1', '2025T2') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE score >= 43.5
      AND TO_DATE(enrolled_at) < DATE '2025-02-24'
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        grade_status_or_unknown,
        student_id,
        SUM(score) FILTER (WHERE grade_status = 'INCOMPLETE') AS score_grade_status_incomplete,
        PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM base
    GROUP BY grade_status_or_unknown, student_id
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM grouped s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status <> 'FAIL'
        )
)
-- Final projection
SELECT grade_status_or_unknown, median_score
FROM qualified
ORDER BY grade_status_or_unknown, median_score DESC NULLS FIRST
