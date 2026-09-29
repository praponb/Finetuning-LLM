WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.score) AS prior_score_7d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(prior_rows_7d) OVER (PARTITION BY prior_rows_7d ORDER BY prior_score_7d NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prior_rows_7d_max_1_prior,
        LEAD(prior_score_7d, 1, 0) OVER (PARTITION BY student_id ORDER BY enrolled_at ASC, id) AS next_prior_score_7d,
        ROUND(prior_score_7d / NULLIF(SUM(prior_score_7d) OVER (PARTITION BY student_id), 0), 4) AS prior_score_7d_share_of_student_id,
        NTILE(10) OVER (PARTITION BY prior_rows_7d ORDER BY prior_rows_7d DESC, id) AS prior_rows_7d_decile
    FROM covered
)
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_7d,
    prior_score_7d,
    prior_rows_7d_max_1_prior,
    next_prior_score_7d,
    prior_score_7d_share_of_student_id,
    prior_rows_7d_decile
FROM windowed
WHERE prior_rows_7d_decile >= 3
ORDER BY prior_rows_7d_max_1_prior DESC, prior_score_7d_share_of_student_id DESC
