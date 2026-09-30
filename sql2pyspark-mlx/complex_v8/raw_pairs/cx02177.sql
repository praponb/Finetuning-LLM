WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND TO_DATE(enrolled_at) < DATE '2025-02-06'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        ROUND(AVG(score) OVER (PARTITION BY course_id, student_id ORDER BY score, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, student_id ORDER BY score), 3) AS score_cume_dist
    FROM cleaned
),
course_id_stats AS (
    -- Avg score per course_id
    SELECT course_id, AVG(score) AS avg_score
    FROM ranked
    GROUP BY course_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM ranked s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score > g.avg_score
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM outliers s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.grade_status IN ('WITHDRAWN', 'INCOMPLETE')
        )
      AND HOUR(s.enrolled_at_quarter) BETWEEN 7 AND 11
)
SELECT id, metric_name, reading
FROM (
    SELECT id, score_share_of_course_id, score, avg_score
    FROM with_activity
) src
UNPIVOT (
    reading FOR metric_name IN (score_share_of_course_id, score, avg_score)
) unp
ORDER BY id, metric_name
