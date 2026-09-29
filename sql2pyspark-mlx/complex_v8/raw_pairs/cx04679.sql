WITH matched AS (
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
    -- Previous / next values per key
    SELECT
        *,
        ROUND(prior_score_12h / NULLIF(SUM(prior_score_12h) OVER (PARTITION BY prior_rows_12h, student_id), 0), 4) AS prior_score_12h_share_of_prior_rows_12h_student_,
        COUNT(*) OVER (PARTITION BY prior_rows_12h, student_id) AS rows_in_prior_rows_12h_student_id,
        ROUND(AVG(prior_rows_12h) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS prior_rows_12h_avg_1_prior
    FROM matched
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status <> 'WITHDRAWN'
              AND e.score <= 81.4
        )
)
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_12h,
    prior_score_12h,
    prior_rows_12h_avg_1_prior,
    CASE WHEN prior_rows_12h >= 6 THEN 'A' WHEN prior_rows_12h >= 3 THEN 'B' ELSE 'C' END AS prior_rows_12h_tier
FROM without_matches
ORDER BY student_id DESC
