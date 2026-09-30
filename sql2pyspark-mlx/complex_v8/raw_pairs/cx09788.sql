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
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.prior_rows_7d,
        s.prior_score_7d,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term,
        e.score AS enrollment_score
    FROM in_window s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
joined AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.prior_rows_7d,
        s.prior_score_7d,
        s.enrollment_id,
        s.enrollment_course_id,
        s.enrollment_enrolled_at,
        s.enrollment_score,
        c.level AS course_level,
        c.dept AS course_dept,
        c.course_code
    FROM line_level s
    JOIN courses c ON c.id = s.enrollment_course_id
)
SELECT
    enrollment_id,
    course_dept,
    course_level,
    MAX(enrollment_enrolled_at) AS last_enrollment_enrolled_at,
    ROUND(AVG(enrollment_score), 2) AS avg_enrollment_score
FROM joined
GROUP BY ROLLUP(enrollment_id, course_dept, course_level)
ORDER BY enrollment_id NULLS LAST, course_dept NULLS LAST, course_level NULLS LAST
