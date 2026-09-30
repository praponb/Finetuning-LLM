WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at_label,
        st.cohort AS student_cohort,
        st.gpa AS student_gpa
    FROM scoped s
    JOIN students st ON st.id = s.student_id
)
SELECT *
FROM (
    SELECT student_id, student_cohort, grade_status, student_gpa
    FROM with_parent_student
) src
PIVOT (
    COUNT(student_gpa) AS count_v, MAX(student_gpa) AS max_v
    FOR grade_status IN ('FAIL', 'INCOMPLETE')
)
ORDER BY student_id, student_cohort
