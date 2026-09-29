WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE (score < 80.2 OR grade_status = 'PASS')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id, term ORDER BY score), 3) AS score_percent_rank,
        LAG(score, 2) OVER (ORDER BY score DESC, id) AS prev2_score,
        NTH_VALUE(score, 2) OVER (PARTITION BY student_id, course_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        MAX(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_max_3_prior,
        LEAD(score, 1, 0) OVER w_course_id_term_by_score AS next_score,
        MAX(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_max_2,
        NTILE(3) OVER w_all_by_score AS score_tercile,
        SIZE(COLLECT_SET(course_id) OVER w_grade_status) AS distinct_course_id_in_grade_status,
        RANK() OVER (PARTITION BY student_id, course_id ORDER BY score DESC) AS score_rank,
        COUNT(*) OVER (PARTITION BY grade_status, term) AS rows_in_grade_status_term,
        SUM(score) OVER (PARTITION BY grade_status) AS grade_status_sum_score
    FROM prepared
    WINDOW w_course_id_term_by_score AS (PARTITION BY course_id, term ORDER BY score, id), w_all_by_score AS (ORDER BY score DESC, id), w_grade_status AS (PARTITION BY grade_status)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(prev2_score, score_percent_rank) AS greatest_prev2_score_score_percent_rank,
        ABS(score - 69.6) AS score_distance,
        FLOOR(grade_status_sum_score / 250) * 250 AS grade_status_sum_score_bucket
    FROM with_trends
    WHERE second_score > 20.2
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(grade_status_sum_score) OVER (PARTITION BY distinct_course_id_in_grade_status, student_id ORDER BY score_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_grade_status_sum_score_so_far,
        second_score - LAG(second_score) OVER (PARTITION BY distinct_course_id_in_grade_status, student_id ORDER BY score_distance DESC, id) AS second_score_change,
        SUM(second_score) OVER (PARTITION BY course_id) AS course_id_sum_second_score
    FROM scored
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    term_grade_status_label,
    score_percent_rank,
    prev2_score,
    second_score,
    next_score,
    score_max_2,
    score_tercile,
    score_rank,
    rows_in_grade_status_term,
    grade_status_sum_score,
    greatest_prev2_score_score_percent_rank,
    score_distance,
    grade_status_sum_score_bucket,
    min_grade_status_sum_score_so_far,
    second_score_change,
    course_id_sum_second_score
FROM with_windows
ORDER BY grade_status DESC
