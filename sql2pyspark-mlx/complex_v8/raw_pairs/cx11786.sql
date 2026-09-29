WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE term NOT IN ('2025T2', '2025T1')
),
combined_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on,
        st.is_international AS student_is_international,
        c.credits AS course_credits
    FROM valid s
    JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_label,
        course_credits,
        SIZE(COLLECT_SET(grade_status_or_unknown) OVER w_student_is_international_stu) AS distinct_grade_status_or_unknown_in_student_is_i,
        ROUND(AVG(course_credits) OVER (PARTITION BY student_id, student_is_international ORDER BY course_credits NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS course_credits_avg_3_prior,
        ROUND(AVG(score) OVER (ORDER BY course_credits DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER (PARTITION BY student_is_international, student_cohort), 0), 4) AS course_credits_share_of_student_is_international,
        SIZE(COLLECT_SET(student_cohort) OVER (PARTITION BY term)) AS distinct_student_cohort_in_term,
        DENSE_RANK() OVER w_course_credits_by_enrolled_a AS enrolled_at_dense_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_is_international, student_cohort), 0), 4) AS score_share_of_student_is_international_student_,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY term), 0), 4) AS score_share_of_term,
        LEAD(course_credits) OVER (ORDER BY course_credits DESC NULLS FIRST, id) AS next_course_credits
    FROM combined_details_student
    WINDOW w_student_is_international_stu AS (PARTITION BY student_is_international, student_cohort), w_course_credits_by_enrolled_a AS (PARTITION BY course_credits ORDER BY enrolled_at NULLS LAST, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY enrolled_at_dense_rank)) AS distinct_course_id_in_enrolled_at_dense_rank,
        COUNT(*) OVER (PARTITION BY enrolled_at_dense_rank, distinct_grade_status_or_unknown_in_student_is_i) AS rows_in_enrolled_at_dense_rank_distinct_grade_st,
        SUM(course_credits) OVER w_enrolled_at_dense_rank AS enrolled_at_dense_rank_sum_course_credits,
        MAX(next_course_credits) OVER (PARTITION BY distinct_grade_status_or_unknown_in_student_is_i, next_course_credits ORDER BY distinct_grade_status_or_unknown_in_student_is_i ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS next_course_credits_max_7,
        ROUND(AVG(score_share_of_term) OVER (PARTITION BY distinct_grade_status_or_unknown_in_student_is_i, next_course_credits ORDER BY distinct_grade_status_or_unknown_in_student_is_i ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_share_of_term_avg_2,
        LEAD(course_credits_avg_3_prior, 3, 0) OVER (PARTITION BY enrolled_at_dense_rank ORDER BY enrolled_at_dense_rank, id) AS next3_course_credits_avg_3_prior
    FROM sequenced
    WINDOW w_enrolled_at_dense_rank AS (PARTITION BY enrolled_at_dense_rank)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    enrolled_at,
    course_credits,
    distinct_grade_status_or_unknown_in_student_is_i,
    course_credits_avg_3_prior,
    score_avg_2,
    course_credits_share_of_student_is_international,
    distinct_student_cohort_in_term,
    enrolled_at_dense_rank,
    score_share_of_student_is_international_student_,
    score_share_of_term,
    next_course_credits,
    rows_in_enrolled_at_dense_rank_distinct_grade_st,
    enrolled_at_dense_rank_sum_course_credits,
    next_course_credits_max_7,
    score_share_of_term_avg_2,
    next3_course_credits_avg_3_prior,
    LENGTH(TRIM(term)) AS term_len,
    LOWER(TRIM(enrolled_at_label)) AS enrolled_at_label_clean
FROM with_windows
ORDER BY next3_course_credits_avg_3_prior DESC NULLS FIRST
