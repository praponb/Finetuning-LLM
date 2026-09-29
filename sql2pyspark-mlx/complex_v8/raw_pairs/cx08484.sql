WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label,
        ABS(score - 38.5) AS score_distance,
        ABS(score - 57.8) AS score_distance_2
    FROM enrollments
    WHERE student_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(score_distance) OVER (ORDER BY score_distance_2 ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_distance_avg_3_prior,
        ROUND(score_distance - AVG(score_distance) OVER (PARTITION BY course_id), 2) AS score_distance_vs_course_id_avg,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER (PARTITION BY course_id), 0), 4) AS score_distance_share_of_course_id
    FROM base
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM metrics s
)
SELECT id, metric_name, value
FROM (
    SELECT id, score_distance, score_distance_2, score_distance_avg_3_prior
    FROM with_stats
) src
UNPIVOT (
    value FOR metric_name IN (score_distance, score_distance_2, score_distance_avg_3_prior)
) unp
ORDER BY id, metric_name
