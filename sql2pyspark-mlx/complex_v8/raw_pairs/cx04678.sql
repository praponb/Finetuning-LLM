WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        UPPER(TRIM(grade_status)) AS grade_status_clean,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref,
        CASE WHEN score >= 57.6 THEN 'large' WHEN score >= 36.6 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE (grade_status = 'FAIL' OR student_id >= 6)
),
combined_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_tier,
        c.credits AS course_credits,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international
    FROM scoped_enrollments s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
)
SELECT
    course_id,
    course_credits,
    GROUPING(course_id) AS is_total_course_id,
    MIN(student_gpa) AS min_student_gpa
FROM combined_details_course
GROUP BY GROUPING SETS ((course_id, course_credits), (course_id), (course_credits))
ORDER BY course_id NULLS LAST, course_credits NULLS LAST
