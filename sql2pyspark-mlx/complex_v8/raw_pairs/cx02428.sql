WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE term IN ('2024T2', '2024T1', '2025T1')
),
combined_details AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa
    FROM recent s
    JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(score) OVER (PARTITION BY MONTH(enrolled_at)) AS enrolled_at_month_sum_score,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        SUM(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_sum_6_prior,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id, grade_status)) AS distinct_term_in_course_id_grade_status,
        RANK() OVER (ORDER BY student_gpa, id) AS student_gpa_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY score ASC, id), 3) AS score_percent_rank,
        ROUND(PERCENT_RANK() OVER w_course_id_enrolled_at_month, 3) AS score_percent_rank_2,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        COUNT(*) OVER (PARTITION BY course_id, grade_status) AS rows_in_course_id_grade_status,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, term ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_cume_dist
    FROM combined_details
    WINDOW w_course_id_enrolled_at_month AS (PARTITION BY course_id, DATE_TRUNC('MONTH', enrolled_at) ORDER BY score DESC)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        student_is_international,
        student_gpa,
        enrolled_at_month_sum_score,
        rows_in_student_id,
        score_sum_6_prior,
        distinct_term_in_course_id_grade_status,
        score_percent_rank,
        score_percent_rank_2,
        score_share_of_student_id,
        rows_in_course_id_grade_status,
        enrolled_at_cume_dist
    FROM sequenced
    WHERE student_gpa_rank <= 5
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(score) OVER (PARTITION BY term, student_is_international ORDER BY score ASC, id) AS prev_score,
        ROUND(PERCENT_RANK() OVER w_all_by_rows_in_student_id_ti, 3) AS rows_in_student_id_percent_rank,
        LAG(enrolled_at_cume_dist, 2, 0) OVER (PARTITION BY term, student_is_international ORDER BY score ASC, id) AS prev2_enrolled_at_cume_dist
    FROM top_rows
    WINDOW w_all_by_rows_in_student_id_ti AS (ORDER BY rows_in_student_id)
)
SELECT
    id,
    student_id,
    term,
    score,
    enrolled_at,
    student_is_international,
    student_gpa,
    enrolled_at_month_sum_score,
    distinct_term_in_course_id_grade_status,
    score_percent_rank,
    score_share_of_student_id,
    enrolled_at_cume_dist,
    prev_score,
    rows_in_student_id_percent_rank,
    prev2_enrolled_at_cume_dist
FROM with_trends
ORDER BY prev2_enrolled_at_cume_dist DESC NULLS FIRST, id
LIMIT 10
