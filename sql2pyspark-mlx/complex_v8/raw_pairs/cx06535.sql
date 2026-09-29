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
unique_rows AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        enrolled_at,
        prior_rows_1d,
        prior_score_1d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY prior_rows_1d DESC, id DESC) AS row_num
        FROM in_window
    ) ranked
    WHERE row_num = 1
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        st.is_international AS student_is_international,
        st.major AS student_major,
        st.gpa AS student_gpa
    FROM unique_rows s
    LEFT JOIN students st ON st.id = s.student_id
),
screened AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM with_parent_student s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.cohort = 'C2021'
              AND st.gpa <= 3.2
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_at, student_id, student_major, student_gpa
    FROM screened
) src
PIVOT (
    MIN(student_gpa)
    FOR student_major IN ('Bio', 'Biz', 'Math', 'Art')
)
ORDER BY enrolled_at, student_id
