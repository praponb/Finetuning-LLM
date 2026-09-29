WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND enrolled_at >= TIMESTAMP '2025-02-06 10:00:00'
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM prepared_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term <> '2025T1'
              AND e.student_id >= 5
        )
      AND s.course_id IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, score, score_adj
    FROM qualified
) src
UNPIVOT (
    metric_value FOR field IN (score, score_adj)
) unp
