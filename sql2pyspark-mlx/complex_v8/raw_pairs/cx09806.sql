WITH covered AS (
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
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at
    FROM covered s
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        prior_rows_1d,
        student_id,
        COUNT(*) AS cnt,
        MIN(enrolled_at) AS first_enrolled_at
    FROM profiled
    WHERE prior_score_1d <= 325.0
    GROUP BY prior_rows_1d, student_id
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.prior_rows_1d,
        s.student_id,
        s.cnt,
        s.first_enrolled_at,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id
    FROM per_group s
    JOIN enrollments e ON e.student_id = s.student_id AND e.course_id IS NOT NULL
)
SELECT prior_rows_1d, cnt, enrollment_score
FROM with_children_enrollments
ORDER BY enrollment_score DESC, prior_rows_1d
