WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE score < 83.1
      AND enrolled_at < TIMESTAMP '2025-02-12 09:00:00'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        NTH_VALUE(score, 2) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        LAG(enrolled_at) OVER (ORDER BY enrolled_at, id) AS prev_enrolled_at,
        SUM(score) OVER (PARTITION BY student_id ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS score_sum_1_prior,
        SUM(score) OVER (PARTITION BY student_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        SUM(score) OVER (PARTITION BY student_id ORDER BY score, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_sum_4,
        MAX(score) OVER (PARTITION BY grade_status) AS grade_status_max_score,
        ROUND(CUME_DIST() OVER w_all_by_enrolled_at_ties, 3) AS enrolled_at_cume_dist,
        NTILE(4) OVER (PARTITION BY student_id ORDER BY score, id) AS score_quartile,
        ROUND(score / NULLIF(SUM(score) OVER w_grade_status, 0), 4) AS score_share_of_grade_status,
        LEAD(score, 2) OVER (PARTITION BY student_id ORDER BY score, id) AS next2_score,
        NTILE(10) OVER (ORDER BY enrolled_at, id) AS enrolled_at_decile,
        LAST_VALUE(score) IGNORE NULLS OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        SUM(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score_2
    FROM filtered
    WINDOW w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at), w_grade_status AS (PARTITION BY grade_status)
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        score_sum_1_prior,
        running_score,
        score_sum_4,
        grade_status_max_score,
        score_quartile,
        score_share_of_grade_status,
        next2_score,
        enrolled_at_decile,
        last_known_score,
        running_score_2,
        ROUND(enrolled_at_decile / NULLIF(SUM(enrolled_at_decile) OVER (PARTITION BY score_quartile, course_id), 0), 4) AS enrolled_at_decile_share_of_score_quartile_cours,
        ROUND(score_sum_1_prior / NULLIF(SUM(score_sum_1_prior) OVER w_score_quartile_course_id, 0), 4) AS score_sum_1_prior_share_of_score_quartile_course,
        NTILE(3) OVER (ORDER BY running_score_2 DESC, id) AS running_score_2_tercile,
        MAX(running_score_2) OVER (ORDER BY last_known_score ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS running_score_2_max_1_prior,
        MAX(running_score) OVER w_score_quartile_course_id AS score_quartile_course_id_max_running_score,
        MIN(enrolled_at_cume_dist) OVER (ORDER BY last_known_score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_enrolled_at_cume_dist_so_far,
        SIZE(COLLECT_SET(student_id) OVER w_score_quartile_course_id) AS distinct_student_id_in_score_quartile_course_id,
        ROUND(running_score / NULLIF(SUM(running_score) OVER (PARTITION BY score_quartile, course_id), 0), 4) AS running_score_share_of_score_quartile_course_id
    FROM with_trends
    WINDOW w_score_quartile_course_id AS (PARTITION BY score_quartile, course_id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    running_score,
    score_sum_4,
    grade_status_max_score,
    score_quartile,
    score_share_of_grade_status,
    next2_score,
    enrolled_at_decile,
    last_known_score,
    running_score_2,
    running_score_2_tercile,
    score_quartile_course_id_max_running_score,
    min_enrolled_at_cume_dist_so_far,
    distinct_student_id_in_score_quartile_course_id,
    running_score_share_of_score_quartile_course_id
FROM with_trends_2
WHERE running_score < 900.0
ORDER BY last_known_score NULLS LAST, score_share_of_grade_status
