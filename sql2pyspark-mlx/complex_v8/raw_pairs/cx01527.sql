WITH matched AS (
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
student_id_stats AS (
    -- Max prior_rows_7d per student_id
    SELECT student_id, MAX(prior_rows_7d) AS max_prior_rows_7d
    FROM matched
    GROUP BY student_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_rows_7d,
        ROUND(s.prior_rows_7d - g.max_prior_rows_7d, 2) AS prior_rows_7d_minus_max
    FROM matched s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.prior_rows_7d = g.max_prior_rows_7d
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        enrolled_at,
        prior_rows_7d,
        prior_score_7d,
        prior_rows_7d_minus_max,
        prior_score_7d + max_prior_rows_7d AS prior_score_7d_plus_max_prior_rows_7d,
        ROUND(max_prior_rows_7d * 1.1, 2) AS max_prior_rows_7d_adj,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM outliers
    WHERE prior_rows_7d_minus_max >= -5.6
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, prior_rows_7d_minus_max, prior_score_7d_plus_max_prior_rows_7d, prior_score_7d
    FROM annotated
) src
UNPIVOT (
    metric_value FOR measure IN (prior_rows_7d_minus_max, prior_score_7d_plus_max_prior_rows_7d, prior_score_7d)
) unp
