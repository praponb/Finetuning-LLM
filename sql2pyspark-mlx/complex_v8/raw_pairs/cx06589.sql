WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE (score >= 35.8 OR term <> '2024T1')
),
with_parent_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        c.course_code
    FROM base s
    JOIN courses c ON c.id = s.course_id
    WHERE UPPER(TRIM(c.course_code)) LIKE 'C%'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY course_id, student_id ORDER BY score, id) AS score_quartile,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        LEAD(score) OVER (PARTITION BY course_id, student_id ORDER BY score, id) AS next_score,
        DENSE_RANK() OVER w_all_by_score_ties AS score_dense_rank,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY student_id)) AS distinct_course_id_in_student_id,
        NTILE(4) OVER (ORDER BY score NULLS LAST, id) AS score_quartile_2,
        FIRST_VALUE(score) IGNORE NULLS OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY score DESC), 3) AS score_percent_rank,
        ROUND(AVG(score) OVER (PARTITION BY course_id), 2) AS course_id_avg_score,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY course_id, student_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score
    FROM with_parent_course
    WINDOW w_course_id AS (PARTITION BY course_id), w_all_by_score_ties AS (ORDER BY score NULLS LAST)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        score_quartile,
        next_score,
        score_dense_rank,
        score_quartile_2,
        first_known_score,
        score_percent_rank,
        last_known_score,
        LEAST(score, last_known_score) AS least_score_last_known_score
    FROM ranked
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(score_dense_rank) OVER (PARTITION BY score_dense_rank ORDER BY score_quartile_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_dense_rank_avg_7,
        LAG(last_known_score, 2) OVER (ORDER BY least_score_last_known_score DESC, id) AS prev2_last_known_score,
        DENSE_RANK() OVER w_all_by_score_ties_3 AS score_dense_rank_2,
        FIRST_VALUE(score_quartile_2) IGNORE NULLS OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score_quartile_2,
        ROUND(AVG(least_score_last_known_score) OVER (PARTITION BY score_dense_rank ORDER BY score_quartile_2, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS least_score_last_known_score_avg_2,
        SUM(score_quartile) OVER (ORDER BY least_score_last_known_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_quartile,
        SUM(first_known_score) OVER (PARTITION BY course_id) AS course_id_sum_first_known_score,
        SUM(score_percent_rank) OVER (PARTITION BY score_dense_rank) AS score_dense_rank_sum_score_percent_rank
    FROM scored
    WINDOW w_all_by_score_ties_3 AS (ORDER BY score)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    next_score,
    score_dense_rank,
    score_quartile_2,
    first_known_score,
    score_percent_rank,
    least_score_last_known_score,
    score_dense_rank_avg_7,
    prev2_last_known_score,
    score_dense_rank_2,
    first_known_score_quartile_2,
    least_score_last_known_score_avg_2,
    running_score_quartile,
    course_id_sum_first_known_score,
    score_dense_rank_sum_score_percent_rank,
    FLOOR(first_known_score / 25) * 25 AS first_known_score_bucket
FROM windowed
ORDER BY running_score_quartile DESC, id NULLS LAST
