WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        ABS(score - 62.5) AS score_distance,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM enrollments
    WHERE course_id IS NOT NULL
),
combined_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.score_distance,
        s.enrolled_at_label,
        c.course_code,
        c.credits AS course_credits,
        c.level AS course_level
    FROM scoped_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        course_id,
        term,
        grade_status,
        score_distance,
        enrolled_at_label,
        course_code,
        course_level,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id)) AS distinct_student_id_in_course_id,
        COUNT(*) OVER (PARTITION BY course_level) AS rows_in_course_level,
        LEAD(score_distance, 1, 0) OVER (ORDER BY score_distance, id) AS next_score_distance,
        ROUND(PERCENT_RANK() OVER w_term_by_score_distance_ties, 3) AS score_distance_percent_rank,
        COUNT(*) OVER w_course_credits AS rows_in_course_credits,
        ROUND(score_distance - AVG(score_distance) OVER (PARTITION BY course_id), 2) AS score_distance_vs_course_id_avg,
        DENSE_RANK() OVER w_all_by_score_distance_ties AS score_distance_dense_rank,
        ROUND(AVG(course_credits) OVER (ORDER BY score_distance, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS course_credits_avg_3_prior,
        SIZE(COLLECT_SET(course_level) OVER w_course_credits) AS distinct_course_level_in_course_credits,
        MAX(course_credits) OVER (PARTITION BY student_id, term ORDER BY score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_course_credits_so_far
    FROM combined_details_course
    WINDOW w_term_by_score_distance_ties AS (PARTITION BY term ORDER BY score_distance DESC), w_course_credits AS (PARTITION BY course_credits), w_all_by_score_distance_ties AS (ORDER BY score_distance)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term,
        grade_status,
        score_distance,
        course_code,
        score_distance_percent_rank,
        score_distance_vs_course_id_avg,
        score_distance_dense_rank,
        course_credits_avg_3_prior,
        max_course_credits_so_far,
        ROUND(course_credits_avg_3_prior / NULLIF(SUM(course_credits_avg_3_prior) OVER (PARTITION BY term), 0), 4) AS course_credits_avg_3_prior_share_of_term,
        NTH_VALUE(next_score_distance, 2) OVER (PARTITION BY term ORDER BY distinct_course_level_in_course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next_score_distance,
        NTILE(3) OVER (PARTITION BY term ORDER BY distinct_course_level_in_course_credits DESC, id) AS distinct_course_level_in_course_credits_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY term ORDER BY distinct_course_level_in_course_credits DESC), 3) AS distinct_course_level_in_course_credits_cume_dis,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY term)) AS distinct_course_id_in_term,
        LEAD(score_distance_dense_rank, 3, 0) OVER w_term_by_distinct_course_leve_2 AS next3_score_distance_dense_rank
    FROM metrics
    WINDOW w_term_by_distinct_course_leve_2 AS (PARTITION BY term ORDER BY distinct_course_level_in_course_credits DESC, id)
)
-- Final projection
SELECT
    id,
    course_id,
    grade_status,
    score_distance,
    score_distance_percent_rank,
    score_distance_vs_course_id_avg,
    score_distance_dense_rank,
    course_credits_avg_3_prior,
    max_course_credits_so_far,
    course_credits_avg_3_prior_share_of_term,
    second_next_score_distance,
    distinct_course_level_in_course_credits_tercile,
    distinct_course_level_in_course_credits_cume_dis,
    distinct_course_id_in_term,
    next3_score_distance_dense_rank
FROM windowed
ORDER BY course_credits_avg_3_prior_share_of_term
