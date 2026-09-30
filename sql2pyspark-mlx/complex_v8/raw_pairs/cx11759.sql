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
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(prior_rows_12h / NULLIF(SUM(prior_rows_12h) OVER (PARTITION BY prior_rows_12h, student_id), 0), 4) AS prior_rows_12h_share_of_prior_rows_12h_student_i,
        prior_rows_12h - LAG(prior_rows_12h) OVER (ORDER BY enrolled_at, id) AS prior_rows_12h_change,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist
    FROM in_window
),
student_id_stats AS (
    -- Avg prior_rows_12h_change per student_id
    SELECT student_id, AVG(prior_rows_12h_change) AS avg_prior_rows_12h_change
    FROM metrics
    GROUP BY student_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_12h_change,
        ROUND(s.prior_rows_12h_change - g.avg_prior_rows_12h_change, 2) AS prior_rows_12h_change_minus_avg
    FROM metrics s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.prior_rows_12h_change > g.avg_prior_rows_12h_change
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_12h,
    prior_score_12h,
    prior_rows_12h_share_of_prior_rows_12h_student_i,
    prior_rows_12h_change,
    prior_rows_12h_change_minus_avg,
    prior_rows_12h_change + prior_score_12h AS prior_rows_12h_change_plus_prior_score_12h
FROM compared
WHERE prior_rows_12h BETWEEN 1 AND 7
ORDER BY id DESC
