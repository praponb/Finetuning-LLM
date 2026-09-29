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
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        enrolled_at,
        prior_score_1d,
        MAX(prior_rows_1d) OVER (PARTITION BY prior_rows_1d) AS prior_rows_1d_max_prior_rows_1d,
        SUM(prior_score_1d) OVER (ORDER BY prior_score_1d ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prior_score_1d_sum_7
    FROM in_window
)
-- Final projection
SELECT
    id,
    prior_score_1d,
    prior_rows_1d_max_prior_rows_1d,
    prior_score_1d_sum_7
FROM with_windows
ORDER BY id, prior_rows_1d_max_prior_rows_1d
