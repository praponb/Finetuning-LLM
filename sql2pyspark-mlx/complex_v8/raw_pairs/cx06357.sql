WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE student_id <= 19
      AND MONTH(enrolled_at) IN (1, 2)
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.enrolled_at >= TIMESTAMP '2025-02-06 10:00:00'
              AND e.score < 85.8
        )
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        score,
        grade_status,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, term ORDER BY days_since_enrolled_at DESC, id), 3) AS days_since_enrolled_at_percent_rank
    FROM with_activity
),
grade_status_stats AS (
    -- Max score per grade_status
    SELECT grade_status, MAX(score) AS max_score
    FROM ranked
    GROUP BY grade_status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_score, ROUND(s.score - g.max_score, 2) AS score_minus_max
    FROM ranked s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score = g.max_score
)
SELECT id, metric, metric_value
FROM (
    SELECT id, days_since_enrolled_at_percent_rank, max_score, score_minus_max, score
    FROM above_average
) src
UNPIVOT (
    metric_value FOR metric IN (days_since_enrolled_at_percent_rank, max_score, score_minus_max, score)
) unp
ORDER BY id, metric
