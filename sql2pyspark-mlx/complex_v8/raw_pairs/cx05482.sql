WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        ROUND(score * 0.25, 2) AS score_adj,
        ABS(score - 57.6) AS score_distance
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 08:00:00'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        enrolled_at,
        score_adj,
        score_distance,
        ROUND(AVG(score_adj) OVER (PARTITION BY term), 2) AS term_avg_score_adj,
        SUM(score_adj) OVER w_term AS term_sum_score_adj,
        MAX(score_adj) OVER (PARTITION BY course_id, student_id) AS course_id_student_id_max_score_adj,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER (PARTITION BY grade_status, term), 0), 4) AS score_distance_share_of_grade_status_term,
        RANK() OVER w_all_by_score_adj AS score_adj_rank,
        MAX(score_distance) OVER (PARTITION BY grade_status, term ORDER BY score_distance DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_distance_max_3_prior,
        ROUND(AVG(score_adj) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', enrolled_at) ORDER BY score_distance ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_adj_avg_3_prior,
        MAX(score_distance) OVER (PARTITION BY course_id, student_id) AS course_id_student_id_max_score_distance,
        LEAD(score_distance, 1, 0) OVER (PARTITION BY grade_status, term ORDER BY score_distance DESC, id) AS next_score_distance,
        LAG(score_adj) OVER (PARTITION BY grade_status, DATE_TRUNC('MONTH', enrolled_at) ORDER BY score_distance ASC NULLS LAST, id) AS prev_score_adj,
        ROUND(PERCENT_RANK() OVER w_enrolled_at_month_by_enrolle, 3) AS enrolled_at_percent_rank,
        ROUND(CUME_DIST() OVER w_grade_status_term_by_score_d_3, 3) AS score_distance_cume_dist,
        RANK() OVER w_student_id_by_enrolled_at_ti AS enrolled_at_rank
    FROM cleaned_enrollments
    WINDOW w_term AS (PARTITION BY term), w_all_by_score_adj AS (ORDER BY score_adj, id), w_enrolled_at_month_by_enrolle AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY enrolled_at DESC), w_grade_status_term_by_score_d_3 AS (PARTITION BY grade_status, term ORDER BY score_distance DESC), w_student_id_by_enrolled_at_ti AS (PARTITION BY student_id ORDER BY enrolled_at ASC NULLS LAST)
)
SELECT
    id,
    enrolled_at,
    score_adj,
    score_distance,
    term_avg_score_adj,
    term_sum_score_adj,
    score_distance_share_of_grade_status_term,
    score_adj_rank,
    score_distance_max_3_prior,
    score_adj_avg_3_prior,
    course_id_student_id_max_score_distance,
    next_score_distance,
    prev_score_adj,
    enrolled_at_percent_rank,
    score_distance_cume_dist,
    enrolled_at_rank
FROM windowed
ORDER BY score_adj_avg_3_prior
