WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE (term IS NOT NULL OR student_id IS NOT NULL)
),
with_details_student AS (
    -- Join students
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.major AS student_major,
        st.enrolled_on AS student_enrolled_on,
        c.dept AS course_dept,
        c.course_code
    FROM recent s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
)
SELECT id, field, amount
FROM (
    SELECT id, score, score_filled
    FROM with_details_student
) src
UNPIVOT (
    amount FOR field IN (score, score_filled)
) unp
