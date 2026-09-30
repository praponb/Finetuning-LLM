WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE student_id IS NOT NULL
),
joined_student AS (
    -- Join students
    SELECT
        s.*,
        st.major AS student_major,
        c.level AS course_level,
        c.course_code
    FROM recent s
    LEFT JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        s.enrolled_at_label,
        s.student_major,
        s.course_level,
        c.dept AS course_dept,
        st.enrolled_on AS student_enrolled_on,
        st.student_name
    FROM joined_student s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.dept IS NOT NULL
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM with_details_course s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
SELECT
    student_id,
    course_id,
    enrollment_count,
    GROUPING(student_id) AS is_total_student_id,
    ARRAY_SORT(COLLECT_SET(course_dept)) AS course_dept_list,
    ROUND(AVG(score), 2) AS avg_score
FROM profiled
GROUP BY CUBE(student_id, course_id, enrollment_count)
