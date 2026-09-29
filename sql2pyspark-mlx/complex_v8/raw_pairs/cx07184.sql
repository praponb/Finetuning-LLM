WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-18 11:00:00'
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        c.level AS course_level,
        c.course_code,
        c.credits AS course_credits,
        st.student_name,
        st.is_international AS student_is_international
    FROM base_enrollments s
    JOIN courses c ON c.id = s.course_id
    JOIN students st ON st.id = s.student_id
)
SELECT
    course_credits,
    COALESCE(grade_status, 'ALL') AS grade_status,
    COALESCE(course_level, 'ALL') AS course_level,
    GROUPING_ID() AS gid,
    ROUND(SUM(score) / NULLIF(SUM(course_credits), 0), 3) AS score_to_course_credits_ratio
FROM joined_course
GROUP BY CUBE(course_credits, grade_status, course_level)
ORDER BY course_credits NULLS LAST, grade_status NULLS LAST, course_level NULLS LAST
