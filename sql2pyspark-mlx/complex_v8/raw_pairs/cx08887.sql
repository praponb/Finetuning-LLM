WITH matched AS (
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
student_id_stats AS (
    -- Avg prior_score_1d per student_id
    SELECT student_id, AVG(prior_score_1d) AS avg_prior_score_1d
    FROM matched
    GROUP BY student_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_score_1d,
        ROUND(s.prior_score_1d - g.avg_prior_score_1d, 2) AS prior_score_1d_minus_avg
    FROM matched s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.prior_score_1d > g.avg_prior_score_1d
)
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_1d,
    avg_prior_score_1d,
    prior_score_1d_minus_avg,
    ROUND(prior_score_1d_minus_avg * 0.9, 2) AS prior_score_1d_minus_avg_adj,
    DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
FROM above_average
ORDER BY student_id DESC
