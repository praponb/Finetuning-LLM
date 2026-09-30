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
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM matched s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
            WHERE e.course_id IS NOT NULL
              AND e.grade_status <> 'WITHDRAWN'
        )
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        prior_rows_1d,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        student_id,
        MAX(enrolled_at) AS last_enrolled_at,
        MIN(prior_rows_1d) AS min_prior_rows_1d,
        MIN(enrolled_at) AS first_enrolled_at,
        COUNT(*) AS row_count
    FROM qualified
    GROUP BY prior_rows_1d, DATE_TRUNC('MONTH', enrolled_at), student_id
)
SELECT
    prior_rows_1d,
    enrolled_at_month,
    min_prior_rows_1d,
    first_enrolled_at,
    row_count,
    ROUND(row_count / NULLIF(prior_rows_1d, 0), 3) AS row_count_per_prior_rows_1d
FROM totals
WHERE row_count < 10
ORDER BY first_enrolled_at
