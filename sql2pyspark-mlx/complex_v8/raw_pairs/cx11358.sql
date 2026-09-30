WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ROUND(score * 1.1, 2) AS score_adj,
        ROUND(score * 0.9, 2) AS score_adj_2
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
),
with_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_adj,
        s.score_adj_2,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international,
        st.student_name,
        c.credits AS course_credits,
        c.level AS course_level
    FROM recent_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
with_details_2 AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        s.score_adj_2,
        s.student_cohort,
        s.student_is_international,
        s.student_name,
        s.course_level,
        c.level AS course_level_2,
        c.credits AS course_credits_2,
        st.cohort AS student_cohort_2,
        st.enrolled_on AS student_enrolled_on
    FROM with_details s
    JOIN courses c ON c.id = s.course_id AND c.course_code IS NOT NULL
    JOIN students st ON st.id = s.student_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    enrolled_at,
    score_adj_2,
    student_cohort,
    student_name,
    course_level_2,
    course_credits_2,
    student_enrolled_on,
    DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year
FROM with_details_2
WHERE student_is_international = TRUE
ORDER BY score_adj_2 NULLS LAST, student_name DESC
