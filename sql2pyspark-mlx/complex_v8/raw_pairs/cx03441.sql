WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.1, 2) AS score_adj,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE student_id IS NOT NULL
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM prepared_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term <> '2024T2'
        )
      AND s.score_adj < 97.5
),
grade_status_stats AS (
    -- Max score_adj per grade_status
    SELECT grade_status, MAX(score_adj) AS max_score_adj
    FROM screened
    GROUP BY grade_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_score_adj,
        ROUND(s.score_adj - g.max_score_adj, 2) AS score_adj_minus_max
    FROM screened s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score_adj = g.max_score_adj
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS max_enrollment_score
    FROM outliers s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, max_enrollment_score, score_adj
    FROM summarized_rows
) src
UNPIVOT (
    metric_value FOR metric_name IN (max_enrollment_score AS m_max_enrollment_score, score_adj AS m_score_adj)
) unp
