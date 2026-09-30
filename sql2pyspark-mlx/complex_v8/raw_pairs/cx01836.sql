WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 38.1) AS score_distance,
        ROUND(score * 0.9, 2) AS score_adj
    FROM enrollments
    WHERE grade_status <> 'PASS'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_distance,
        score_adj,
        SUM(score_distance) OVER (PARTITION BY YEAR(enrolled_at), term ORDER BY score_distance ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_distance_sum_3_prior,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        MIN(score) OVER (PARTITION BY course_id) AS course_id_min_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY term, student_id ORDER BY score), 3) AS score_percent_rank,
        MAX(score_distance) OVER w_course_id AS course_id_max_score_distance,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id, student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        DENSE_RANK() OVER (PARTITION BY term, student_id ORDER BY score) AS score_dense_rank
    FROM valid_enrollments
    WINDOW w_course_id AS (PARTITION BY course_id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE score_dense_rank = 1
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    score_distance,
    score_adj,
    score_distance_sum_3_prior,
    first_known_enrolled_at,
    rows_in_course_id,
    course_id_min_score,
    score_percent_rank,
    course_id_max_score_distance,
    score_dense_rank,
    ROUND(score_percent_rank / NULLIF(score_adj, 0), 3) AS score_percent_rank_per_score_adj,
    SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix
FROM top_rows
WHERE HOUR(first_known_enrolled_at) BETWEEN 8 AND 12
ORDER BY id DESC NULLS FIRST
