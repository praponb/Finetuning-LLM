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
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM in_window
    WHERE prior_rows_1d < (SELECT MAX(prior_rows_1d) FROM in_window)
)
SELECT
    id,
    enrolled_at,
    prior_rows_1d,
    prior_score_1d,
    FLOOR(prior_score_1d / 100) * 100 AS prior_score_1d_bucket
FROM screened
ORDER BY prior_rows_1d
