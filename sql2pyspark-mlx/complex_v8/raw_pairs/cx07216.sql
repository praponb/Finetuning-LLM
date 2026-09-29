WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        UPPER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE term = '2024T1'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        st.major AS student_major
    FROM scoped s
    JOIN students st ON st.id = s.student_id
),
with_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        c.dept AS course_dept,
        c.level AS course_level
    FROM with_details s
    JOIN courses c ON c.id = s.course_id AND c.dept NOT IN ('Bio', 'Biz')
    WHERE c.dept IS NOT NULL
)
SELECT
    course_id,
    student_id,
    SUM(CASE WHEN grade_status = 'FAIL' THEN 1 ELSE 0 END) AS n_grade_status_fail,
    COUNT(*) AS cnt
FROM with_details_course
GROUP BY GROUPING SETS ((course_id, student_id), (course_id), (student_id), ())
