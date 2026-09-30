WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.score) AS prior_score_3d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM in_window s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score < 59.8
        )
      AND s.prior_rows_3d <= 6
),
combined_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_rows_3d,
        s.prior_score_3d,
        st.major AS student_major
    FROM qualified s
    JOIN students st ON st.id = s.student_id
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM combined_details_student s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.enrolled_at < TIMESTAMP '2025-03-02 08:00:00'
        )
)
-- Final projection
SELECT
    id,
    student_id,
    enrolled_at,
    prior_rows_3d,
    prior_score_3d,
    student_major,
    DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
FROM filtered
ORDER BY student_id DESC, id DESC NULLS FIRST
