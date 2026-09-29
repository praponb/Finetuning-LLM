WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term IN ('2024T2', '2025T1')
),
joined AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        st.gpa AS student_gpa,
        st.major AS student_major,
        c.course_code,
        c.level AS course_level
    FROM scoped_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
grade_status_stats AS (
    -- Avg student_gpa per grade_status
    SELECT grade_status, AVG(student_gpa) AS avg_student_gpa
    FROM joined
    GROUP BY grade_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_student_gpa,
        ROUND(s.student_gpa - g.avg_student_gpa, 2) AS student_gpa_minus_avg
    FROM joined s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.student_gpa > g.avg_student_gpa
)
SELECT
    course_id,
    course_level,
    student_major,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt
FROM outliers
GROUP BY CUBE(course_id, course_level, student_major)
ORDER BY course_id NULLS LAST, course_level NULLS LAST, student_major NULLS LAST
