WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status IN ('INCOMPLETE', 'WITHDRAWN')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY enrolled_at DESC) AS enrolled_at_rank,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_id)) AS distinct_grade_status_in_course_id,
        LEAD(score, 2) OVER (ORDER BY score NULLS LAST, id) AS next2_score,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id)) AS distinct_term_in_course_id,
        ROUND(score - AVG(score) OVER w_course_id, 2) AS score_vs_course_id_avg,
        MIN(score) OVER (PARTITION BY course_id) AS course_id_min_score,
        RANK() OVER (ORDER BY score ASC) AS score_rank,
        MAX(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_max_3,
        ROUND(PERCENT_RANK() OVER w_all_by_enrolled_at_ties, 3) AS enrolled_at_percent_rank,
        SUM(score) OVER (PARTITION BY course_id) AS course_id_sum_score,
        ROUND(AVG(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS score_avg_4,
        NTH_VALUE(score, 2) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        LEAD(enrolled_at) OVER (ORDER BY score NULLS LAST, id) AS next_enrolled_at,
        COUNT(*) OVER w_course_id AS rows_in_course_id
    FROM recent
    WINDOW w_course_id AS (PARTITION BY course_id), w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at DESC)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        enrolled_at_rank,
        distinct_grade_status_in_course_id,
        next2_score,
        distinct_term_in_course_id,
        score_vs_course_id_avg,
        course_id_min_score,
        score_rank,
        score_max_3,
        course_id_sum_score,
        next_enrolled_at,
        rows_in_course_id,
        SUM(course_id_sum_score) OVER (PARTITION BY grade_status, score_rank) AS grade_status_score_rank_sum_course_id_sum_score,
        MIN(score_avg_4) OVER (PARTITION BY grade_status, score_rank) AS grade_status_score_rank_min_score_avg_4,
        SUM(enrolled_at_rank) OVER (PARTITION BY enrolled_at_rank, grade_status ORDER BY score_avg_4 DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS enrolled_at_rank_sum_7,
        COUNT(*) OVER (PARTITION BY enrolled_at_rank, grade_status ORDER BY score_avg_4 DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        RANK() OVER (ORDER BY enrolled_at ASC) AS enrolled_at_rank_2,
        DENSE_RANK() OVER (ORDER BY enrolled_at ASC, id) AS enrolled_at_dense_rank,
        COUNT(*) OVER (PARTITION BY grade_status, score_rank) AS rows_in_grade_status_score_rank
    FROM windowed
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    enrolled_at_rank,
    distinct_grade_status_in_course_id,
    next2_score,
    score_vs_course_id_avg,
    course_id_min_score,
    score_rank,
    score_max_3,
    grade_status_score_rank_sum_course_id_sum_score,
    enrolled_at_rank_sum_7,
    running_count,
    enrolled_at_rank_2,
    enrolled_at_dense_rank,
    GREATEST(enrolled_at_dense_rank, course_id_min_score) AS greatest_enrolled_at_dense_rank_course_id_min_sc,
    ROUND(course_id_sum_score * 1.1, 2) AS course_id_sum_score_adj
FROM with_trends
ORDER BY course_id_min_score
