WITH joined_ranges AS (
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
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        prior_rows_7d,
        student_id,
        COUNT(*) AS cnt,
        MIN(prior_score_7d) AS min_prior_score_7d,
        MAX(enrolled_at) AS last_enrolled_at
    FROM joined_ranges
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
combined_details AS (
    -- Join students
    SELECT s.*, st.cohort AS student_cohort
    FROM per_group s
    LEFT JOIN students st ON st.id = s.student_id AND UPPER(TRIM(st.student_name)) LIKE 'H%'
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.enrolled_at_month,
        s.prior_rows_7d,
        s.student_id,
        s.cnt,
        s.min_prior_score_7d,
        s.last_enrolled_at,
        s.student_cohort,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM combined_details s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
)
-- Final projection
SELECT
    enrolled_at_month,
    prior_rows_7d,
    student_id,
    cnt,
    last_enrolled_at,
    enrollment_term,
    enrollment_course_id,
    enrollment_enrolled_at,
    ROUND(min_prior_score_7d / NULLIF(prior_rows_7d, 0), 3) AS min_prior_score_7d_per_prior_rows_7d,
    TO_DATE(enrollment_enrolled_at) AS enrollment_enrolled_at_date
FROM with_children_enrollments
ORDER BY enrollment_term, enrolled_at_month DESC
