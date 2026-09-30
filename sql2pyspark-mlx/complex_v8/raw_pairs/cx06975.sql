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
joined_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_score_1d,
        st.gpa AS student_gpa
    FROM in_window s
    JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at,
    student_gpa,
    DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
    MONTH(enrolled_at) AS enrolled_at_month
FROM joined_student
WHERE HOUR(enrolled_at) BETWEEN 7 AND 10
ORDER BY student_id NULLS LAST, id
LIMIT 20
