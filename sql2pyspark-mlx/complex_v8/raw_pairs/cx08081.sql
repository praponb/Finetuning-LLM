WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-07 11:00:00'
      AND grade_status IN ('PASS', 'INCOMPLETE', 'WITHDRAWN')
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_bucket,
        st.major AS student_major,
        st.student_name,
        st.gpa AS student_gpa
    FROM scoped_enrollments s
    JOIN students st ON st.id = s.student_id
    WHERE UPPER(TRIM(st.student_name)) LIKE 'A%'
),
combined_details_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.score_bucket,
        s.student_major,
        s.student_name,
        st.student_name AS student_student_name,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM combined_details s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
)
-- Subtotals with GROUPING SETS
SELECT
    student_id,
    course_credits,
    SUM(score_bucket) AS total_score_bucket
FROM combined_details_2
GROUP BY GROUPING SETS ((student_id, course_credits), (student_id), (course_credits), ())
