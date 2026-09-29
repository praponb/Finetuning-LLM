WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        MONTH(enrolled_at) AS enrolled_at_month_2,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE course_id <= 3
),
with_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at_month_2,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on
    FROM scoped s
    JOIN students st ON st.id = s.student_id
)
SELECT *
FROM (
    SELECT student_enrolled_on, student_id, grade_status, score
    FROM with_details_student
) src
PIVOT (
    AVG(score) AS avg_v, MAX(score) AS max_v
    FOR grade_status IN ('FAIL', 'WITHDRAWN', 'PASS', 'INCOMPLETE')
)
ORDER BY student_enrolled_on, student_id
