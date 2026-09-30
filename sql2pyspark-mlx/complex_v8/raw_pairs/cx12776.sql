WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-19 07:00:00'
      AND grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'PASS')
),
with_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.score_filled,
        c.credits AS course_credits,
        c.dept AS course_dept,
        st.enrolled_on AS student_enrolled_on,
        st.is_international AS student_is_international
    FROM cleaned_enrollments s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.dept IN ('Bio', 'Art', 'Biz')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY term, course_credits)) AS distinct_course_id_in_term_course_credits,
        ROUND(score_filled / NULLIF(SUM(score_filled) OVER (PARTITION BY term, course_credits), 0), 4) AS score_filled_share_of_term_course_credits,
        COUNT(*) OVER (PARTITION BY term, course_credits) AS rows_in_term_course_credits,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY term, course_credits)) AS distinct_grade_status_in_term_course_credits,
        ROUND(score_filled - AVG(score_filled) OVER (PARTITION BY term, course_credits), 2) AS score_filled_vs_term_course_credits_avg,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER w_student_enrolled_on_month, 0), 4) AS course_credits_share_of_student_enrolled_on_mont,
        DENSE_RANK() OVER w_student_is_international_by AS course_credits_dense_rank
    FROM with_details_course
    WINDOW w_student_enrolled_on_month AS (PARTITION BY MONTH(student_enrolled_on)), w_student_is_international_by AS (PARTITION BY student_is_international ORDER BY course_credits DESC)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    score_filled,
    course_credits,
    course_dept,
    student_is_international,
    rows_in_term_course_credits,
    distinct_grade_status_in_term_course_credits,
    score_filled_vs_term_course_credits_avg,
    course_credits_share_of_student_enrolled_on_mont,
    course_credits_dense_rank,
    COALESCE(score_filled, 0.0) AS score_filled_filled,
    CASE WHEN student_is_international THEN 1 ELSE 0 END AS student_is_international_flag
FROM sequenced
ORDER BY course_credits, term
