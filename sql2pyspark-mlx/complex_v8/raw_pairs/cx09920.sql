WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        UPPER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE course_id <= 7
      AND HOUR(enrolled_at) BETWEEN 7 AND 12
),
combined_details_course AS (
    -- Enrich with dimension data
    SELECT s.*, c.credits AS course_credits, st.enrolled_on AS student_enrolled_on
    FROM scoped s
    LEFT JOIN courses c ON c.id = s.course_id AND LOWER(c.course_code) LIKE '%i3%'
    LEFT JOIN students st ON st.id = s.student_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        score_bucket,
        grade_status_clean,
        course_credits,
        MIN(score_bucket) OVER (ORDER BY course_credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_bucket_so_far,
        ROUND(score_bucket - AVG(score_bucket) OVER w_grade_status, 2) AS score_bucket_vs_grade_status_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term, grade_status ORDER BY student_enrolled_on DESC, id), 3) AS student_enrolled_on_percent_rank,
        DENSE_RANK() OVER (ORDER BY student_enrolled_on, id) AS student_enrolled_on_dense_rank,
        LEAD(score_bucket, 3, 0) OVER w_course_credits_by_enrolled_a AS next3_score_bucket,
        LAG(score_bucket, 1, 0) OVER (PARTITION BY term, grade_status ORDER BY student_enrolled_on DESC, id) AS prev_score_bucket,
        NTH_VALUE(course_credits, 2) OVER (ORDER BY course_credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_course_credits,
        ROUND(AVG(score_bucket) OVER w_grade_status, 2) AS grade_status_avg_score_bucket,
        LAST_VALUE(score_bucket) IGNORE NULLS OVER (PARTITION BY course_credits ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_bucket,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER (PARTITION BY grade_status), 0), 4) AS course_credits_share_of_grade_status,
        SIZE(COLLECT_SET(course_id) OVER w_enrolled_at_month_course_cre) AS distinct_course_id_in_enrolled_at_month_course_c,
        ROUND(score - AVG(score) OVER w_enrolled_at_month_course_cre, 2) AS score_vs_enrolled_at_month_course_credits_avg,
        LEAD(course_credits, 3) OVER (ORDER BY student_enrolled_on, id) AS next3_course_credits,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_credits ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        COUNT(*) OVER w_grade_status AS rows_in_grade_status
    FROM combined_details_course
    WINDOW w_grade_status AS (PARTITION BY grade_status), w_course_credits_by_enrolled_a AS (PARTITION BY course_credits ORDER BY enrolled_at, id), w_enrolled_at_month_course_cre AS (PARTITION BY MONTH(enrolled_at), course_credits)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        score_bucket,
        grade_status_clean,
        course_credits,
        min_score_bucket_so_far,
        score_bucket_vs_grade_status_avg,
        student_enrolled_on_percent_rank,
        next3_score_bucket,
        prev_score_bucket,
        second_course_credits,
        grade_status_avg_score_bucket,
        last_known_score_bucket,
        course_credits_share_of_grade_status,
        distinct_course_id_in_enrolled_at_month_course_c,
        score_vs_enrolled_at_month_course_credits_avg,
        next3_course_credits,
        second_score,
        rows_in_grade_status
    FROM metrics
    WHERE student_enrolled_on_dense_rank = 1
      AND distinct_course_id_in_enrolled_at_month_course_c >= 4
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        score,
        enrolled_at,
        grade_status_clean,
        course_credits,
        min_score_bucket_so_far,
        score_bucket_vs_grade_status_avg,
        student_enrolled_on_percent_rank,
        next3_score_bucket,
        last_known_score_bucket,
        distinct_course_id_in_enrolled_at_month_course_c,
        next3_course_credits,
        second_score,
        ROUND(AVG(score_bucket) OVER (PARTITION BY course_id, next3_course_credits), 2) AS course_id_next3_course_credits_avg_score_bucket,
        score - LAG(score) OVER (PARTITION BY term ORDER BY second_score, id) AS score_change,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id, next3_course_credits)) AS distinct_term_in_course_id_next3_course_credits,
        NTH_VALUE(next3_score_bucket, 2) OVER (PARTITION BY course_id ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next3_score_bucket
    FROM top_per_group
)
SELECT
    id,
    student_id,
    score,
    enrolled_at,
    course_credits,
    min_score_bucket_so_far,
    score_bucket_vs_grade_status_avg,
    student_enrolled_on_percent_rank,
    next3_score_bucket,
    distinct_course_id_in_enrolled_at_month_course_c,
    next3_course_credits,
    second_score,
    distinct_term_in_course_id_next3_course_credits
FROM ranked
ORDER BY distinct_term_in_course_id_next3_course_credits DESC
