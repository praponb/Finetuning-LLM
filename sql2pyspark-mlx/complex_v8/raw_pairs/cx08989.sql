WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE grade_status <> 'FAIL'
      AND term IN ('2024T1', '2024T2')
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM valid_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term NOT IN ('2025T1', '2024T2')
              AND e.score BETWEEN 11.1 AND 66.0
        )
      AND s.grade_status <> 'WITHDRAWN'
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        grade_status,
        grade_status_or_unknown,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(score), 2) AS total_score,
        PERCENTILE_APPROX(score, 0.5) AS median_score,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
    FROM screened
    WHERE student_id >= 10
    GROUP BY DATE_TRUNC('MONTH', enrolled_at), grade_status, grade_status_or_unknown
    HAVING SUM(score) > 8.8
)
SELECT
    enrolled_at_month,
    grade_status_or_unknown,
    distinct_ids,
    total_score,
    median_score,
    CONCAT(grade_status_or_unknown, '#', CAST(distinct_ids AS STRING)) AS grade_status_or_unknown_ref
FROM stats
