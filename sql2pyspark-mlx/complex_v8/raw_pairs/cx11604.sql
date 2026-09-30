WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.score) AS prior_score_3d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
student_id_stats AS (
    -- Max prior_rows_3d per student_id
    SELECT student_id, MAX(prior_rows_3d) AS max_prior_rows_3d
    FROM in_window
    GROUP BY student_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_rows_3d,
        ROUND(s.prior_rows_3d - g.max_prior_rows_3d, 2) AS prior_rows_3d_minus_max
    FROM in_window s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.prior_rows_3d = g.max_prior_rows_3d
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM compared s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM summarized_rows
    WHERE prior_rows_3d > (SELECT MIN(prior_rows_3d) FROM summarized_rows)
)
SELECT id, measure, reading
FROM (
    SELECT id, prior_rows_3d_minus_max, max_prior_rows_3d, prior_score_3d
    FROM without_matches
) src
UNPIVOT (
    reading FOR measure IN (prior_rows_3d_minus_max AS prior_rows_3d_minus_max_v, max_prior_rows_3d AS m_max_prior_rows_3d, prior_score_3d AS m_prior_score_3d)
) unp
ORDER BY id, measure
