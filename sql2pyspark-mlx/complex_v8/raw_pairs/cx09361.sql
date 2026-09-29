WITH covered AS (
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
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM covered s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
            WHERE TO_DATE(e.enrolled_at) < DATE '2025-03-08'
        )
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.term AS enrollment_term
    FROM without_matches s
    JOIN enrollments e ON e.student_id = s.student_id AND e.term = '2024T1'
)
SELECT id, enrolled_at, enrollment_term
FROM exploded_detail_enrollments
WHERE enrollment_course_id >= 3
