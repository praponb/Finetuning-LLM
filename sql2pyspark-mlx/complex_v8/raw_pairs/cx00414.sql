WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE grade_status = 'FAIL'
),
with_parent_student AS (
    -- Join students
    SELECT
        s.*,
        st.major AS student_major,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international
    FROM prepared s
    JOIN students st ON st.id = s.student_id AND st.gpa < 3.4
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.student_is_international,
        st.student_name,
        st.enrolled_on AS student_enrolled_on
    FROM with_parent_student s
    JOIN students st ON st.id = s.student_id
    WHERE UPPER(TRIM(st.student_name)) LIKE 'H%'
)
SELECT
    id,
    term,
    score,
    student_is_international,
    student_name,
    student_enrolled_on,
    DATE_TRUNC('WEEK', student_enrolled_on) AS student_enrolled_on_week
FROM joined
ORDER BY term, student_name
