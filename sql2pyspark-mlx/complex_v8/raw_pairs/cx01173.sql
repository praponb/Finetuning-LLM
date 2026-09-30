WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        YEAR(enrolled_at) AS enrolled_at_year,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE student_id >= 2
),
with_parent AS (
    -- Attach student attributes
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa
    FROM scoped_enrollments s
    JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(enrolled_at_year) OVER (ORDER BY student_gpa NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS enrolled_at_year_max_3_prior,
        ROUND(PERCENT_RANK() OVER w_student_id_term_by_score_tie, 3) AS score_percent_rank,
        student_gpa - LAG(student_gpa) OVER (PARTITION BY student_id, term ORDER BY score, id) AS student_gpa_change,
        ROUND(AVG(score) OVER (ORDER BY student_gpa NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2,
        MIN(student_gpa) OVER (PARTITION BY student_id, term ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_student_gpa_so_far,
        MAX(enrolled_at_year) OVER (PARTITION BY student_id, term ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS enrolled_at_year_max_1_prior,
        student_gpa - LAG(student_gpa) OVER (PARTITION BY course_id ORDER BY student_gpa DESC, id) AS student_gpa_change_2,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id, term ORDER BY score), 3) AS score_cume_dist
    FROM with_parent
    WINDOW w_student_id_term_by_score_tie AS (PARTITION BY student_id, term ORDER BY score)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    enrolled_at_year,
    grade_status_ref,
    student_is_international,
    student_gpa,
    enrolled_at_year_max_3_prior,
    score_percent_rank,
    student_gpa_change,
    score_avg_2,
    student_gpa_change_2
FROM windowed
ORDER BY score_percent_rank, score_avg_2 DESC
