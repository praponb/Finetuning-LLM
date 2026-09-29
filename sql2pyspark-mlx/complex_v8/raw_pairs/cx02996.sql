WITH base_enrollments AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
    WHERE student_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(score) OVER w_grade_status_term AS grade_status_term_min_score,
        MAX(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_max_1_prior,
        RANK() OVER (PARTITION BY course_id ORDER BY enrolled_at) AS enrolled_at_rank,
        COUNT(*) OVER w_grade_status_term AS rows_in_grade_status_term,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY term, student_id)) AS distinct_grade_status_in_term_student_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term ORDER BY enrolled_at DESC), 3) AS enrolled_at_percent_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status, term), 0), 4) AS score_share_of_grade_status_term,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, course_id), 2) AS score_vs_grade_status_course_id_avg,
        score - LAG(score) OVER (PARTITION BY course_id, TO_DATE(enrolled_at) ORDER BY enrolled_at, id) AS score_change,
        MAX(score) OVER (PARTITION BY term ORDER BY enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_max_2
    FROM base_enrollments
    WINDOW w_grade_status_term AS (PARTITION BY grade_status, term)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE enrolled_at_rank <= 5
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(enrolled_at_percent_rank) OVER w_student_id, 2) AS student_id_avg_enrolled_at_percent_rank,
        MAX(enrolled_at_rank) OVER (PARTITION BY term, distinct_grade_status_in_term_student_id ORDER BY grade_status_term_min_score DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS enrolled_at_rank_max_2,
        LEAD(enrolled_at_percent_rank) OVER (PARTITION BY MONTH(enrolled_at), distinct_grade_status_in_term_student_id ORDER BY score_vs_grade_status_course_id_avg ASC, id) AS next_enrolled_at_percent_rank,
        NTILE(5) OVER w_enrolled_at_month_distinct_g AS score_vs_grade_status_course_id_avg_quintile
    FROM top_per_group
    WINDOW w_student_id AS (PARTITION BY student_id), w_enrolled_at_month_distinct_g AS (PARTITION BY MONTH(enrolled_at), distinct_grade_status_in_term_student_id ORDER BY score_vs_grade_status_course_id_avg ASC, id)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    grade_status_term_min_score,
    score_max_1_prior,
    distinct_grade_status_in_term_student_id,
    enrolled_at_percent_rank,
    score_share_of_grade_status_term,
    score_vs_grade_status_course_id_avg,
    score_change,
    student_id_avg_enrolled_at_percent_rank,
    enrolled_at_rank_max_2,
    score_vs_grade_status_course_id_avg_quintile,
    ROUND(score_change / NULLIF(enrolled_at_percent_rank, 0), 3) AS score_change_per_enrolled_at_percent_rank,
    TO_DATE(enrolled_at) AS enrolled_at_date
FROM metrics_2
ORDER BY score_max_1_prior DESC
