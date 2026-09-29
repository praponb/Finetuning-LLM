WITH in_window AS (
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
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        prior_rows_12h,
        prior_score_12h,
        LAG(prior_rows_12h) OVER w_prior_rows_12h_by_prior_scor AS prev_prior_rows_12h,
        SUM(prior_score_12h) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prior_score_12h_sum_2_prior,
        LEAD(prior_rows_12h) OVER (PARTITION BY prior_rows_12h ORDER BY prior_score_12h ASC, id) AS next_prior_rows_12h,
        prior_score_12h - LAG(prior_score_12h) OVER (ORDER BY enrolled_at DESC, id) AS prior_score_12h_change,
        ROUND(prior_rows_12h - AVG(prior_rows_12h) OVER (PARTITION BY prior_rows_12h, student_id), 2) AS prior_rows_12h_vs_prior_rows_12h_student_id_avg,
        ROUND(prior_rows_12h / NULLIF(SUM(prior_rows_12h) OVER w_prior_rows_12h_student_id, 0), 4) AS prior_rows_12h_share_of_prior_rows_12h_student_i
    FROM in_window
    WINDOW w_prior_rows_12h_by_prior_scor AS (PARTITION BY prior_rows_12h ORDER BY prior_score_12h ASC, id), w_prior_rows_12h_student_id AS (PARTITION BY prior_rows_12h, student_id)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM ranked
    WHERE prev_prior_rows_12h > (SELECT MIN(prev_prior_rows_12h) FROM ranked)
)
SELECT
    next_prior_rows_12h,
    prev_prior_rows_12h,
    prior_rows_12h,
    COUNT(DISTINCT id) AS distinct_ids
FROM screened
GROUP BY CUBE(next_prior_rows_12h, prev_prior_rows_12h, prior_rows_12h)
ORDER BY next_prior_rows_12h NULLS LAST, prev_prior_rows_12h NULLS LAST, prior_rows_12h NULLS LAST
