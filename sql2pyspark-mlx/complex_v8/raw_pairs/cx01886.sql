WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-03-02 08:00:00'
),
joined_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at_week,
        st.gpa AS student_gpa,
        st.enrolled_on AS student_enrolled_on,
        c.dept AS course_dept,
        c.course_code
    FROM scoped s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        student_enrolled_on,
        course_dept,
        course_code,
        DATE_FORMAT(enrolled_at_week, 'MMM yyyy') AS enrolled_at_week_label
    FROM joined_student
),
combined_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_dept,
        s.course_code,
        s.enrolled_at_week_label,
        st.cohort AS student_cohort
    FROM derived s
    JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    student_id,
    course_code,
    student_cohort
FROM combined_details_student
ORDER BY id
