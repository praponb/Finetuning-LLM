WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE student_id <= 17
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        score_adj,
        grade_status_ref,
        DENSE_RANK() OVER (ORDER BY enrolled_at DESC) AS enrolled_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(AVG(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_percent_rank,
        ROUND(score_adj / NULLIF(SUM(score_adj) OVER (PARTITION BY term), 0), 4) AS score_adj_share_of_term,
        score - LAG(score) OVER w_grade_status_student_id_by_e AS score_change,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        SUM(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_sum_2_prior,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        SUM(score) OVER (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY term)) AS distinct_student_id_in_term,
        RANK() OVER w_grade_status_student_id_by_e AS enrolled_at_rank,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY term)) AS distinct_course_id_in_term
    FROM filtered
    WINDOW w_grade_status_student_id_by_e AS (PARTITION BY grade_status, student_id ORDER BY enrolled_at DESC NULLS FIRST, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM ranked
    WHERE enrolled_at_dense_rank <= 3
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(enrolled_at_percent_rank / NULLIF(SUM(enrolled_at_percent_rank) OVER (PARTITION BY grade_status, course_id), 0), 4) AS enrolled_at_percent_rank_share_of_grade_status_c,
        ROUND(score_adj - AVG(score_adj) OVER (PARTITION BY distinct_student_id_in_term, student_id), 2) AS score_adj_vs_distinct_student_id_in_term_student,
        DENSE_RANK() OVER (PARTITION BY term, distinct_course_id_in_term ORDER BY running_score ASC NULLS LAST) AS running_score_dense_rank
    FROM top_per_group
)
SELECT
    id,
    student_id,
    course_id,
    score,
    score_adj,
    grade_status_ref,
    enrolled_at_dense_rank,
    minutes_since_prev_enrolled_at,
    score_avg_6_prior,
    enrolled_at_percent_rank,
    score_adj_share_of_term,
    score_change,
    first_enrolled_at,
    rows_in_term,
    running_score,
    enrolled_at_rank,
    distinct_course_id_in_term,
    enrolled_at_percent_rank_share_of_grade_status_c,
    score_adj_vs_distinct_student_id_in_term_student,
    running_score_dense_rank
FROM metrics
ORDER BY id DESC, distinct_course_id_in_term DESC NULLS FIRST
