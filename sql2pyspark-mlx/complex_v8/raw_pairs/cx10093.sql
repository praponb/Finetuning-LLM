WITH joined_ranges AS (
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
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        enrolled_at,
        prior_rows_12h,
        prior_score_12h,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        MAX(prior_rows_12h) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prior_rows_12h_max_2,
        ROUND(AVG(prior_score_12h) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS prior_score_12h_avg_7,
        MIN(prior_score_12h) OVER (PARTITION BY student_id) AS student_id_min_prior_score_12h
    FROM joined_ranges
),
student_id_stats AS (
    -- Max prior_rows_12h_max_2 per student_id
    SELECT student_id, MAX(prior_rows_12h_max_2) AS max_prior_rows_12h_max_2
    FROM metrics
    GROUP BY student_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_rows_12h_max_2,
        ROUND(s.prior_rows_12h_max_2 - g.max_prior_rows_12h_max_2, 2) AS prior_rows_12h_max_2_minus_max
    FROM metrics s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.prior_rows_12h_max_2 = g.max_prior_rows_12h_max_2
),
tagged AS (
    -- Derive helper columns
    SELECT *, DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM compared
    WHERE prior_rows_12h_max_2 < 16
)
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_12h,
    prior_score_12h,
    student_id_min_prior_score_12h,
    max_prior_rows_12h_max_2,
    prior_rows_12h_max_2_minus_max,
    days_since_enrolled_at
FROM tagged
