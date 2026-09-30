WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE grade_status = 'INCOMPLETE'
      AND enrolled_at >= TIMESTAMP '2025-02-12 09:00:00'
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM prepared_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id >= 10
        )
      AND s.term IS NOT NULL
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.score BETWEEN 20.6 AND 94.2
              AND e.grade_status = 'FAIL'
        )
)
-- Subtotals with GROUPING SETS
SELECT
    term,
    student_id,
    COUNT(*) AS n_rows,
    ROUND(SUM(score) / NULLIF(SUM(score_bucket), 0), 3) AS score_to_score_bucket_ratio,
    COUNT(DISTINCT id) AS distinct_ids
FROM screened
GROUP BY GROUPING SETS ((term, student_id), (term), (student_id))
