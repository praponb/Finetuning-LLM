WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 57.8) AS score_distance,
        FLOOR(score / 25) * 25 AS score_bucket,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE score >= 17.4
),
with_parent AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.score_distance,
        s.score_bucket,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international,
        c.course_code
    FROM prepared s
    JOIN students st ON st.id = s.student_id AND st.enrolled_on >= DATE '2024-04-04'
    JOIN courses c ON c.id = s.course_id
    WHERE st.is_international = FALSE
),
with_activity AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.enrolled_on >= DATE '2024-05-18'
              AND st.gpa > 2.5
        )
)
SELECT *
FROM (
    SELECT term, student_id, grade_status, score_bucket
    FROM with_activity
) src
PIVOT (
    MIN(score_bucket)
    FOR grade_status IN ('INCOMPLETE', 'WITHDRAWN')
)
