WITH covered AS (
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
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(enrolled_at) AS enrolled_at_day,
        student_id,
        prior_rows_1d,
        PERCENTILE_APPROX(prior_score_1d, 0.5) AS median_prior_score_1d,
        ROUND(AVG(prior_rows_1d), 2) AS avg_prior_rows_1d,
        COUNT(*) AS row_count
    FROM covered
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    enrolled_at_day,
    student_id,
    prior_rows_1d,
    median_prior_score_1d,
    avg_prior_rows_1d,
    row_count,
    DAYOFWEEK(enrolled_at_day) AS enrolled_at_day_dayofweek
FROM totals
ORDER BY avg_prior_rows_1d NULLS LAST
