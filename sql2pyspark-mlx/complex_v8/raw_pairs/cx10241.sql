WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE student_id IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        c.level AS course_level,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort
    FROM recent s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        score_filled,
        student_cohort,
        LEAD(student_enrolled_on) OVER w_term_course_id_by_score AS next_student_enrolled_on,
        ROUND(AVG(score_filled) OVER (PARTITION BY term, course_id ORDER BY score ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_filled_avg_1_prior,
        DENSE_RANK() OVER (PARTITION BY term, course_id ORDER BY score ASC) AS score_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY term, course_id ORDER BY score ASC, id), 3) AS score_cume_dist,
        ROUND(PERCENT_RANK() OVER w_all_by_score, 3) AS score_percent_rank,
        SIZE(COLLECT_SET(course_level) OVER (PARTITION BY grade_status)) AS distinct_course_level_in_grade_status,
        LAST_VALUE(enrolled_at) OVER (ORDER BY student_enrolled_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        ROUND(score_filled / NULLIF(SUM(score_filled) OVER (PARTITION BY grade_status), 0), 4) AS score_filled_share_of_grade_status,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status), 0), 4) AS score_share_of_grade_status,
        SUM(score) OVER (PARTITION BY term, course_id ORDER BY score ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_sum_2_prior
    FROM with_parent
    WINDOW w_term_course_id_by_score AS (PARTITION BY term, course_id ORDER BY score ASC, id), w_all_by_score AS (ORDER BY score, id)
)
SELECT
    id,
    course_id,
    term,
    student_cohort,
    score_percent_rank,
    distinct_course_level_in_grade_status,
    rows_in_grade_status,
    score_filled_share_of_grade_status,
    score_share_of_grade_status,
    score_sum_2_prior,
    DATE_TRUNC('QUARTER', last_enrolled_at) AS last_enrolled_at_quarter,
    DATE_TRUNC('WEEK', next_student_enrolled_on) AS next_student_enrolled_on_week
FROM sequenced
ORDER BY next_student_enrolled_on_week
