WITH in_window AS (
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
detail_rows_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_score_1d,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score,
        e.term AS enrollment_term
    FROM in_window s
    JOIN enrollments e ON e.student_id = s.student_id
)
SELECT
    id,
    student_id,
    enrolled_at,
    prior_score_1d,
    enrollment_id,
    enrollment_grade_status,
    enrollment_course_id,
    ABS(enrollment_score - 39.4) AS enrollment_score_distance,
    ROUND(enrollment_score / NULLIF(prior_score_1d, 0), 3) AS enrollment_score_per_prior_score_1d
FROM detail_rows_enrollments
WHERE enrollment_id IS NOT NULL
ORDER BY enrollment_score_distance, enrollment_score_per_prior_score_1d NULLS LAST
