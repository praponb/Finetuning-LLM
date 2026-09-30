WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ABS(score - 39.4) AS score_distance
    FROM enrollments
    WHERE grade_status IN ('PASS', 'INCOMPLETE')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        score_distance,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY score_distance) AS score_distance_dense_rank,
        NTH_VALUE(score_distance, 2) OVER (ORDER BY score_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_distance,
        ROUND(score - AVG(score) OVER w_term, 2) AS score_vs_term_avg,
        ROUND(score_distance - AVG(score_distance) OVER (PARTITION BY term), 2) AS score_distance_vs_term_avg,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER w_term, 0), 4) AS score_distance_share_of_term,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        score_distance - LAG(score_distance) OVER w_all_by_score_distance AS score_distance_change,
        LAG(score_distance) OVER w_student_id_term_by_score_dis AS prev_score_distance,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY course_id ORDER BY score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        ROUND(score / NULLIF(SUM(score) OVER w_term, 0), 4) AS score_share_of_term,
        LEAD(score) OVER w_course_id_by_score_distance_3 AS next_score,
        LAG(score_distance, 1, 0) OVER w_all_by_score_distance AS prev_score_distance_2,
        ROUND(CUME_DIST() OVER (ORDER BY score_distance DESC NULLS FIRST), 3) AS score_distance_cume_dist
    FROM cleaned
    WINDOW w_term AS (PARTITION BY term), w_all_by_score_distance AS (ORDER BY score_distance DESC NULLS FIRST, id), w_student_id_term_by_score_dis AS (PARTITION BY student_id, term ORDER BY score_distance DESC, id), w_course_id_by_score_distance_3 AS (PARTITION BY course_id ORDER BY score_distance, id)
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM with_trends
    WHERE score_distance_dense_rank <= 3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY course_id, score_distance_dense_rank ORDER BY score_distance_share_of_term DESC) AS score_distance_share_of_term_rank,
        LAST_VALUE(score_vs_term_avg) OVER (PARTITION BY student_id, grade_status ORDER BY score_distance_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_vs_term_avg,
        ROUND(AVG(prev_score_distance_2) OVER (PARTITION BY score_distance_dense_rank, student_id), 2) AS score_distance_dense_rank_student_id_avg_prev_sc,
        LAST_VALUE(score_distance_share_of_term) IGNORE NULLS OVER (PARTITION BY student_id, grade_status ORDER BY rows_in_term NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_distance_share_of_term,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        ROUND(score_share_of_term - AVG(score_share_of_term) OVER (PARTITION BY grade_status), 2) AS score_share_of_term_vs_grade_status_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, grade_status ORDER BY score_distance_cume_dist DESC), 3) AS score_distance_cume_dist_percent_rank
    FROM first_rows
)
SELECT
    id,
    course_id,
    grade_status,
    score_distance_dense_rank,
    score_distance_share_of_term,
    rows_in_term,
    score_distance_change,
    score_share_of_term,
    next_score,
    prev_score_distance_2,
    score_distance_cume_dist,
    score_distance_share_of_term_rank,
    last_score_vs_term_avg,
    score_distance_dense_rank_student_id_avg_prev_sc,
    score_share_of_term_vs_grade_status_avg,
    score_distance_cume_dist_percent_rank,
    ROUND(score_distance * 0.9, 2) AS score_distance_adj
FROM with_windows
WHERE score_distance_change BETWEEN -40.0 AND 30.0
ORDER BY score_distance_share_of_term, score_share_of_term_vs_grade_status_avg DESC
