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
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        enrolled_at,
        prior_score_1d,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY prior_rows_1d ORDER BY prior_rows_1d NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        LAG(prior_rows_1d) OVER w_prior_rows_1d_by_prior_rows_2 AS prev_prior_rows_1d,
        prior_rows_1d - LAG(prior_rows_1d) OVER (PARTITION BY MONTH(enrolled_at), prior_rows_1d ORDER BY prior_score_1d ASC, id) AS prior_rows_1d_change,
        SUM(prior_score_1d) OVER (PARTITION BY student_id) AS student_id_sum_prior_score_1d
    FROM in_window
    WINDOW w_prior_rows_1d_by_prior_rows_2 AS (PARTITION BY prior_rows_1d ORDER BY prior_rows_1d NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at,
    prior_score_1d,
    prev_prior_rows_1d,
    student_id_sum_prior_score_1d,
    DAYOFWEEK(first_enrolled_at) AS first_enrolled_at_dayofweek
FROM with_trends
