WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.score) AS prior_score_3d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_student_id_prior_rows_3d AS rows_in_student_id_prior_rows_3d,
        ROUND(AVG(prior_score_3d) OVER (PARTITION BY student_id, prior_rows_3d), 2) AS student_id_prior_rows_3d_avg_prior_score_3d,
        COUNT(*) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM joined_ranges
    WINDOW w_student_id_prior_rows_3d AS (PARTITION BY student_id, prior_rows_3d)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(enrolled_at) AS enrolled_at_year,
        MAX(enrolled_at) AS last_enrolled_at,
        PERCENTILE_APPROX(student_id_prior_rows_3d_avg_prior_score_3d, 0.5) AS median_student_id_prior_rows_3d_avg_prior_score_,
        ROUND(SUM(running_count) / NULLIF(SUM(prior_rows_3d), 0), 3) AS running_count_to_prior_rows_3d_ratio,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(rows_in_student_id_prior_rows_3d) AS max_rows_in_student_id_prior_rows_3d
    FROM with_trends
    GROUP BY YEAR(enrolled_at)
    HAVING MAX(student_id_prior_rows_3d_avg_prior_score_3d) > 75.0
),
annotated AS (
    -- Label and score rows
    SELECT
        enrolled_at_year,
        last_enrolled_at,
        median_student_id_prior_rows_3d_avg_prior_score_,
        running_count_to_prior_rows_3d_ratio,
        distinct_ids,
        max_rows_in_student_id_prior_rows_3d,
        DATE_FORMAT(last_enrolled_at, 'yyyy-MM') AS last_enrolled_at_label,
        enrolled_at_year + distinct_ids AS enrolled_at_year_plus_distinct_ids
    FROM grouped
)
-- Final projection
SELECT
    enrolled_at_year,
    last_enrolled_at,
    max_rows_in_student_id_prior_rows_3d,
    last_enrolled_at_label,
    enrolled_at_year_plus_distinct_ids,
    DATEDIFF(DATE '2025-06-30', last_enrolled_at) AS days_since_last_enrolled_at,
    LOWER(REGEXP_REPLACE(TRIM(last_enrolled_at_label), ' ', '_')) AS last_enrolled_at_label_slug
FROM annotated
WHERE max_rows_in_student_id_prior_rows_3d >= 7
ORDER BY last_enrolled_at DESC, enrolled_at_year_plus_distinct_ids DESC NULLS FIRST
