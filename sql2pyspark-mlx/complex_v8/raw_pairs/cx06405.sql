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
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        prior_rows_1d,
        prior_score_1d,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY student_id, TO_DATE(enrolled_at) ORDER BY prior_rows_1d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        MAX(prior_score_1d) OVER (PARTITION BY student_id, TO_DATE(enrolled_at) ORDER BY prior_rows_1d DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prior_score_1d_max_2_prior,
        prior_score_1d - LAG(prior_score_1d) OVER (PARTITION BY student_id, TO_DATE(enrolled_at) ORDER BY prior_rows_1d DESC, id) AS prior_score_1d_change,
        ROUND(prior_rows_1d - AVG(prior_rows_1d) OVER w_student_id, 2) AS prior_rows_1d_vs_student_id_avg,
        SUM(prior_score_1d) OVER (PARTITION BY student_id) AS student_id_sum_prior_score_1d,
        LEAD(prior_score_1d, 3, 0) OVER (PARTITION BY student_id, TO_DATE(enrolled_at) ORDER BY prior_rows_1d DESC, id) AS next3_prior_score_1d
    FROM in_window
    WINDOW w_student_id AS (PARTITION BY student_id)
)
-- Final projection
SELECT
    id,
    prior_rows_1d,
    prior_score_1d,
    prior_score_1d_change,
    prior_rows_1d_vs_student_id_avg
FROM sequenced
WHERE prior_score_1d_max_2_prior <= 1200.0
ORDER BY prior_score_1d NULLS LAST, id
LIMIT 5
