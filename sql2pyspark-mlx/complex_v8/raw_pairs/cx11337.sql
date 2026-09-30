WITH in_window AS (
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
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(score) AS min_enrollment_score,
        MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score,
        e.last_enrollment_enrolled_at
    FROM in_window s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
SELECT
    prior_rows_7d,
    student_id,
    GROUPING(prior_rows_7d) AS is_total_prior_rows_7d,
    COUNT(DISTINCT student_id) AS distinct_student_ids
FROM summarized_rows
GROUP BY ROLLUP(prior_rows_7d, student_id)
