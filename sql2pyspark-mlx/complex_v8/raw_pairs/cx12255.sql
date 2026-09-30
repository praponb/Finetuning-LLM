WITH covered AS (
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
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        prior_rows_3d,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_ym,
        student_id,
        ROUND(AVG(prior_rows_3d), 2) AS avg_prior_rows_3d,
        COUNT(*) AS n_rows
    FROM covered
    GROUP BY 1, 2, 3
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at
    FROM per_group s
)
-- Native SQL UNPIVOT (wide to long)
SELECT prior_rows_3d, enrolled_at_ym, field, reading
FROM (
    SELECT prior_rows_3d, enrolled_at_ym, avg_enrollment_score, avg_prior_rows_3d
    FROM with_counts
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (avg_enrollment_score, avg_prior_rows_3d)
) unp
