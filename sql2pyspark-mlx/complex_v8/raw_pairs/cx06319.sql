WITH recent AS (
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
    WHERE student_id >= 16
),
with_details_course AS (
    -- Join courses
    SELECT
        s.*,
        c.level AS course_level,
        st.gpa AS student_gpa,
        st.student_name
    FROM recent s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(enrolled_at) OVER (PARTITION BY grade_status, course_level ORDER BY student_gpa NULLS LAST, id) AS next_enrolled_at,
        NTH_VALUE(score, 2) OVER (PARTITION BY grade_status, course_level ORDER BY student_gpa NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        ROUND(CUME_DIST() OVER w_grade_status_course_level_by, 3) AS student_gpa_cume_dist,
        MAX(student_gpa) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS student_gpa_max_7,
        ROW_NUMBER() OVER (ORDER BY student_gpa DESC NULLS FIRST, id) AS rn,
        ROW_NUMBER() OVER (PARTITION BY grade_status, course_level ORDER BY student_gpa NULLS LAST, id) AS rn_2,
        ROUND(AVG(score) OVER (ORDER BY student_gpa DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_avg_1_prior,
        DENSE_RANK() OVER (ORDER BY student_gpa DESC NULLS FIRST, id) AS student_gpa_dense_rank,
        DENSE_RANK() OVER (PARTITION BY grade_status, course_level ORDER BY student_gpa NULLS LAST, id) AS student_gpa_dense_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_level, term ORDER BY enrolled_at ASC), 3) AS enrolled_at_percent_rank
    FROM with_details_course
    WINDOW w_grade_status_course_level_by AS (PARTITION BY grade_status, course_level ORDER BY student_gpa NULLS LAST, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY rn_2, grade_status)) AS distinct_term_in_rn_2_grade_status,
        MIN(enrolled_at_percent_rank) OVER (ORDER BY student_gpa_dense_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_enrolled_at_percent_rank_so_far,
        NTILE(3) OVER w_all_by_student_gpa_dense_ran_2 AS student_gpa_dense_rank_tercile,
        COUNT(*) OVER (PARTITION BY course_level) AS rows_in_course_level,
        LAST_VALUE(enrolled_at_percent_rank) OVER (PARTITION BY course_level ORDER BY student_gpa_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at_percent_rank,
        ROUND(enrolled_at_percent_rank - AVG(enrolled_at_percent_rank) OVER (PARTITION BY course_level), 2) AS enrolled_at_percent_rank_vs_course_level_avg,
        COUNT(*) OVER (PARTITION BY rn_2, grade_status) AS rows_in_rn_2_grade_status,
        ROUND(rn / NULLIF(SUM(rn) OVER (PARTITION BY rn_2, grade_status), 0), 4) AS rn_share_of_rn_2_grade_status
    FROM metrics
    WINDOW w_all_by_student_gpa_dense_ran_2 AS (ORDER BY student_gpa_dense_rank ASC, id)
)
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    course_level,
    student_name,
    next_enrolled_at,
    second_score,
    student_gpa_cume_dist,
    student_gpa_max_7,
    rn,
    student_gpa_dense_rank_2,
    min_enrolled_at_percent_rank_so_far,
    student_gpa_dense_rank_tercile,
    rows_in_course_level,
    last_enrolled_at_percent_rank,
    enrolled_at_percent_rank_vs_course_level_avg,
    rows_in_rn_2_grade_status,
    rn_share_of_rn_2_grade_status
FROM with_trends
WHERE HOUR(next_enrolled_at) BETWEEN 10 AND 16
