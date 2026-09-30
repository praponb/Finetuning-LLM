WITH in_window AS (
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
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.enrolled_at AS enrollment_enrolled_at
    FROM in_window s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM line_level s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term IN ('2024T2', '2024T1')
              AND e.grade_status = 'PASS'
        )
),
enriched_student AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.major AS student_major,
        e.term AS enrollment_term_2,
        e.grade_status AS enrollment_grade_status
    FROM screened s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
)
-- Final projection
SELECT
    id,
    enrolled_at,
    enrollment_id,
    enrollment_course_id,
    student_is_international,
    student_major,
    enrollment_grade_status
FROM enriched_student
WHERE enrollment_id >= 7
ORDER BY id
