WITH joined_ranges AS (
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
enriched_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_rows_1d,
        st.gpa AS student_gpa,
        st.enrolled_on AS student_enrolled_on,
        st.student_name
    FROM joined_ranges s
    LEFT JOIN students st ON st.id = s.student_id
),
combined_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.student_gpa,
        s.student_enrolled_on,
        s.student_name,
        st.is_international AS student_is_international
    FROM enriched_student s
    JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    student_id,
    enrolled_at,
    student_gpa,
    student_name,
    student_is_international,
    COALESCE(student_gpa, 0.0) AS student_gpa_filled,
    DATE_FORMAT(student_enrolled_on, 'yyyy-MM-dd') AS student_enrolled_on_label
FROM combined_details_student
ORDER BY student_gpa_filled DESC, student_id DESC
