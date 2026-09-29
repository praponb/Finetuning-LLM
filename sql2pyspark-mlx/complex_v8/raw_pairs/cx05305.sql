WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.score) AS prior_score_1d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
grouped AS (
    -- Summarise per group
    SELECT
        student_id,
        PERCENTILE_APPROX(prior_rows_1d, 0.5) AS median_prior_rows_1d,
        ROUND(STDDEV(prior_rows_1d), 2) AS stddev_prior_rows_1d
    FROM in_window
    GROUP BY student_id
)
-- Final projection
SELECT
    student_id,
    median_prior_rows_1d,
    stddev_prior_rows_1d,
    ABS(median_prior_rows_1d - 4) AS median_prior_rows_1d_distance,
    ROUND(stddev_prior_rows_1d / NULLIF(median_prior_rows_1d, 0), 3) AS stddev_prior_rows_1d_per_median_prior_rows_1d
FROM grouped
