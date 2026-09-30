WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj
    FROM enrollments
    WHERE course_id <= 4
      AND enrolled_at >= TIMESTAMP '2025-02-12 09:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        score,
        enrolled_at,
        score_adj,
        ROUND(score_adj / NULLIF(SUM(score_adj) OVER (PARTITION BY term), 0), 4) AS score_adj_share_of_term,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_id)) AS distinct_term_in_student_id,
        COUNT(*) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(score_adj) OVER (PARTITION BY student_id) AS student_id_sum_score_adj,
        LEAD(enrolled_at) OVER w_course_id_by_score_adj AS next_enrolled_at,
        FIRST_VALUE(score) OVER (PARTITION BY term, course_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        SUM(score_adj) OVER (PARTITION BY term, course_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_adj,
        SUM(score_adj) OVER (PARTITION BY term, course_id ORDER BY score DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_adj_sum_6_prior
    FROM filtered
    WINDOW w_course_id_by_score_adj AS (PARTITION BY course_id ORDER BY score_adj DESC, id)
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(running_count) OVER (PARTITION BY course_id) AS course_id_min_running_count,
        ROW_NUMBER() OVER (PARTITION BY distinct_term_in_student_id, TO_DATE(enrolled_at) ORDER BY next_enrolled_at, id) AS rn,
        student_id_sum_score_adj - LAG(student_id_sum_score_adj) OVER (PARTITION BY distinct_term_in_student_id, course_id ORDER BY running_score_adj DESC, id) AS student_id_sum_score_adj_change
    FROM metrics
)
SELECT
    id,
    score,
    enrolled_at,
    score_adj_share_of_term,
    distinct_term_in_student_id,
    running_count,
    student_id_sum_score_adj,
    next_enrolled_at,
    first_score,
    score_adj_sum_6_prior,
    rn,
    student_id_sum_score_adj_change
FROM metrics_2
WHERE student_id_sum_score_adj_change < 1200.0
ORDER BY student_id_sum_score_adj, next_enrolled_at
