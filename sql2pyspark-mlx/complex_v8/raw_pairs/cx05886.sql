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
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY prior_score_7d ASC NULLS LAST, id) AS prior_score_7d_seq,
        RANK() OVER (ORDER BY prior_score_7d ASC NULLS LAST, id) AS prior_score_7d_rank,
        COUNT(*) OVER w_student_id AS rows_in_student_id,
        ROUND(prior_score_7d / NULLIF(SUM(prior_score_7d) OVER (PARTITION BY student_id), 0), 4) AS prior_score_7d_share_of_student_id,
        ROUND(prior_rows_7d - AVG(prior_rows_7d) OVER (PARTITION BY student_id), 2) AS prior_rows_7d_vs_student_id_avg
    FROM matched
    WINDOW w_student_id AS (PARTITION BY student_id)
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_7d,
    prior_score_7d,
    prior_score_7d_seq,
    prior_score_7d_share_of_student_id,
    prior_rows_7d_vs_student_id_avg,
    DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
FROM with_trends
ORDER BY prior_rows_7d_vs_student_id_avg DESC, student_id DESC
