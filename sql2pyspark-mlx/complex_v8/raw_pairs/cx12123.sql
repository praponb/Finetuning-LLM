WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj
    FROM enrollments
    WHERE student_id IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY score_adj ASC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        LAG(enrolled_at) OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id) AS prev_enrolled_at,
        NTILE(4) OVER w_all_by_score AS score_quartile,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY grade_status ORDER BY score_adj DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at_2,
        SIZE(COLLECT_SET(grade_status) OVER w_student_id) AS distinct_grade_status_in_student_id,
        RANK() OVER (PARTITION BY grade_status, term ORDER BY score_adj) AS score_adj_rank,
        ROUND(score_adj - AVG(score_adj) OVER (PARTITION BY grade_status), 2) AS score_adj_vs_grade_status_avg,
        ROUND(CUME_DIST() OVER w_grade_status_by_score_adj_2, 3) AS score_adj_cume_dist
    FROM base_enrollments
    WINDOW w_all_by_score AS (ORDER BY score DESC, id), w_student_id AS (PARTITION BY student_id), w_grade_status_by_score_adj_2 AS (PARTITION BY grade_status ORDER BY score_adj DESC, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(score) OVER (PARTITION BY MONTH(prev_enrolled_at), score_quartile ORDER BY score_adj_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        RANK() OVER w_prev_enrolled_at_month_score_2 AS score_adj_rank_rank,
        ROUND(score_adj_cume_dist / NULLIF(SUM(score_adj_cume_dist) OVER (PARTITION BY student_id), 0), 4) AS score_adj_cume_dist_share_of_student_id,
        SUM(score) OVER (PARTITION BY student_id) AS student_id_sum_score,
        FIRST_VALUE(score_adj_rank) IGNORE NULLS OVER (ORDER BY score_adj_vs_grade_status_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score_adj_rank,
        LAG(score, 3) OVER w_all_by_score_adj_vs_grade_st_2 AS prev3_score,
        ROW_NUMBER() OVER (ORDER BY score_adj_vs_grade_status_avg ASC, id) AS seq_no
    FROM with_windows
    WINDOW w_prev_enrolled_at_month_score_2 AS (PARTITION BY MONTH(prev_enrolled_at), score_quartile ORDER BY score_adj_rank, id), w_all_by_score_adj_vs_grade_st_2 AS (ORDER BY score_adj_vs_grade_status_avg ASC, id)
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    enrolled_at,
    minutes_since_prev_enrolled_at,
    prev_enrolled_at,
    score_quartile,
    minutes_since_prev_enrolled_at_2,
    distinct_grade_status_in_student_id,
    score_adj_rank,
    score_adj_vs_grade_status_avg,
    score_adj_cume_dist,
    running_score,
    score_adj_rank_rank,
    score_adj_cume_dist_share_of_student_id,
    student_id_sum_score,
    first_known_score_adj_rank,
    seq_no,
    LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
    LENGTH(TRIM(grade_status)) AS grade_status_len
FROM ranked
WHERE score_adj_rank_rank <= 7
ORDER BY score, id DESC
