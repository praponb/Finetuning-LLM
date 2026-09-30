WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.score) AS prior_score_12h
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(prior_score_12h, prior_rows_12h) AS least_prior_score_12h_prior_rows_12h,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM covered
)
SELECT
    id,
    student_id,
    enrolled_at,
    prior_score_12h,
    least_prior_score_12h_prior_rows_12h,
    FLOOR(prior_score_12h / 100) * 100 AS prior_score_12h_bucket,
    ROUND(prior_rows_12h / NULLIF(prior_score_12h, 0), 3) AS prior_rows_12h_per_prior_score_12h
FROM scored
WHERE student_id IS NOT NULL
ORDER BY least_prior_score_12h_prior_rows_12h NULLS LAST
