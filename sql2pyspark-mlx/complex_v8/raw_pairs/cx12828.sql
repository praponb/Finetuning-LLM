WITH valid AS (
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
    WHERE enrolled_at < TIMESTAMP '2025-02-12 12:00:00'
      AND grade_status IS NOT NULL
),
combined_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        c.course_code,
        c.dept AS course_dept,
        st.gpa AS student_gpa
    FROM valid s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
)
SELECT *
FROM (
    SELECT student_id, course_id, course_dept, student_gpa
    FROM combined_details_course
) src
PIVOT (
    COUNT(student_gpa) AS count_v, SUM(student_gpa) AS sum_v
    FOR course_dept IN ('CS', 'Biz')
)
