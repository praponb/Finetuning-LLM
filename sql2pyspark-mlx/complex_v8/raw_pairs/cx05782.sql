WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref,
        ROUND(score * 0.9, 2) AS score_adj
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 2)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        grade_status_ref,
        COUNT(*) OVER w_student_id AS rows_in_student_id,
        SUM(score_adj) OVER (PARTITION BY course_id, term ORDER BY score_adj, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_adj_sum_2,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY student_id)) AS distinct_grade_status_in_student_id,
        FIRST_VALUE(score_adj) IGNORE NULLS OVER (PARTITION BY course_id, term ORDER BY score_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score_adj,
        ROUND(PERCENT_RANK() OVER w_all_by_enrolled_at_ties, 3) AS enrolled_at_percent_rank,
        MIN(score_adj) OVER (PARTITION BY student_id) AS student_id_min_score_adj,
        LEAD(score_adj) OVER w_student_id_term_by_score AS next_score_adj,
        SUM(score) OVER (PARTITION BY student_id) AS student_id_sum_score,
        NTILE(5) OVER (ORDER BY enrolled_at, id) AS enrolled_at_quintile,
        SUM(score) OVER (PARTITION BY course_id, term ORDER BY score_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, term ORDER BY score DESC, id), 3) AS score_percent_rank,
        LEAD(score, 3) OVER w_all_by_enrolled_at AS next3_score,
        score_adj - LAG(score_adj) OVER (PARTITION BY course_id, term ORDER BY score_adj, id) AS score_adj_change
    FROM recent
    WINDOW w_student_id AS (PARTITION BY student_id), w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at), w_student_id_term_by_score AS (PARTITION BY student_id, term ORDER BY score DESC, id), w_all_by_enrolled_at AS (ORDER BY enrolled_at, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status_ref,
    rows_in_student_id,
    score_adj_sum_2,
    distinct_grade_status_in_student_id,
    first_known_score_adj,
    enrolled_at_percent_rank,
    student_id_min_score_adj,
    next_score_adj,
    student_id_sum_score,
    enrolled_at_quintile,
    running_score,
    score_percent_rank,
    score_adj_change,
    CASE WHEN enrolled_at_percent_rank >= 0.8 THEN 'A' WHEN enrolled_at_percent_rank >= 0.5 THEN 'B' ELSE 'C' END AS enrolled_at_percent_rank_tier,
    FLOOR(first_known_score_adj / 25) * 25 AS first_known_score_adj_bucket
FROM windowed
WHERE student_id_min_score_adj > 525.0
ORDER BY enrolled_at_percent_rank NULLS LAST, student_id_sum_score DESC, id
LIMIT 10
