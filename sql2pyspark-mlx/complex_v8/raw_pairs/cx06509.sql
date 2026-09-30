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
enriched AS (
    -- Join students
    SELECT s.*, st.student_name, st.major AS student_major
    FROM covered s
    LEFT JOIN students st ON st.id = s.student_id
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_rows_1d,
        s.student_name,
        st.student_name AS student_student_name
    FROM enriched s
    LEFT JOIN students st ON st.id = s.student_id
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.student_name,
        s.student_student_name,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id
    FROM with_parent_student s
    LEFT JOIN enrollments e ON e.student_id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    student_name,
    enrollment_term,
    enrollment_course_id,
    CONCAT(enrollment_term, '#', CAST(enrollment_id AS STRING)) AS enrollment_term_ref
FROM exploded_detail
WHERE student_id <= 17
ORDER BY enrollment_term DESC NULLS FIRST, student_name DESC NULLS FIRST
