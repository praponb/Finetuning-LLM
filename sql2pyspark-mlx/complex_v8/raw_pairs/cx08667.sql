WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
),
joined_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort
    FROM recent s
    LEFT JOIN students st ON st.id = s.student_id AND st.cohort = 'C2023'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT term, enrolled_at, grade_status, score
    FROM joined_student
) src
PIVOT (
    COUNT(score) AS count_v, SUM(score) AS sum_v
    FOR grade_status IN ('WITHDRAWN', 'INCOMPLETE')
)
ORDER BY term, enrolled_at
