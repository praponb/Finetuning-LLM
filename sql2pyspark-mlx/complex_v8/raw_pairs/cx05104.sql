WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
    WHERE student_id IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(score_adj) OVER (PARTITION BY course_id, grade_status) AS course_id_grade_status_max_score_adj,
        SUM(score_adj) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_adj_sum_3,
        ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY score_adj ASC, id) AS score_adj_seq,
        COUNT(*) OVER w_student_id AS rows_in_student_id,
        LAG(enrolled_at) OVER (PARTITION BY student_id, course_id ORDER BY score_adj, id) AS prev_enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY course_id ORDER BY score_adj ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        RANK() OVER (ORDER BY score DESC NULLS FIRST, id) AS score_rank,
        ROUND(score_adj / NULLIF(SUM(score_adj) OVER (PARTITION BY student_id), 0), 4) AS score_adj_share_of_student_id,
        COUNT(*) OVER (PARTITION BY student_id, course_id ORDER BY score_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(score) OVER (PARTITION BY course_id, grade_status) AS course_id_grade_status_sum_score,
        ROUND(AVG(score_adj) OVER w_course_id_grade_status, 2) AS course_id_grade_status_avg_score_adj,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist,
        SUM(score) OVER (PARTITION BY course_id ORDER BY score_adj ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_sum_4,
        ROUND(score - AVG(score) OVER w_student_id, 2) AS score_vs_student_id_avg,
        ROUND(score_adj - AVG(score_adj) OVER (PARTITION BY student_id), 2) AS score_adj_vs_student_id_avg
    FROM base_enrollments
    WINDOW w_student_id AS (PARTITION BY student_id), w_course_id_grade_status AS (PARTITION BY course_id, grade_status)
)
-- Final projection
SELECT
    id,
    course_id,
    score_adj,
    course_id_grade_status_max_score_adj,
    score_adj_sum_3,
    score_adj_seq,
    rows_in_student_id,
    prev_enrolled_at,
    score_avg_3_prior,
    score_adj_share_of_student_id,
    running_count,
    enrolled_at_cume_dist,
    score_sum_4,
    score_vs_student_id_avg,
    score_adj_vs_student_id_avg,
    LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
FROM with_trends
ORDER BY score_avg_3_prior, id
LIMIT 10
