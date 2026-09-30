WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE term = '2025T1'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at,
        score_filled,
        ROUND(score_filled / NULLIF(SUM(score_filled) OVER (PARTITION BY course_id), 0), 4) AS score_filled_share_of_course_id,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY grade_status, course_id)) AS distinct_student_id_in_grade_status_course_id,
        ROUND(CUME_DIST() OVER w_all_by_score_ties, 3) AS score_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id, MONTH(enrolled_at) ORDER BY score_filled NULLS LAST), 3) AS score_filled_percent_rank,
        MAX(score_filled) OVER w_grade_status_course_id AS grade_status_course_id_max_score_filled,
        score - LAG(score) OVER w_all_by_score AS score_change,
        NTH_VALUE(score_filled, 2) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_filled,
        SUM(score_filled) OVER (PARTITION BY grade_status, course_id) AS grade_status_course_id_sum_score_filled
    FROM prepared_enrollments
    WINDOW w_all_by_score_ties AS (ORDER BY score DESC), w_grade_status_course_id AS (PARTITION BY grade_status, course_id), w_all_by_score AS (ORDER BY score DESC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at,
        score_filled,
        distinct_student_id_in_grade_status_course_id,
        score_cume_dist,
        grade_status_course_id_max_score_filled,
        grade_status_course_id_sum_score_filled,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id)) AS distinct_student_id_in_course_id,
        LAST_VALUE(score_cume_dist) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_cume_dist,
        SIZE(COLLECT_SET(course_id) OVER w_distinct_student_id_in_grade) AS distinct_course_id_in_distinct_student_id_in_gra
    FROM windowed
    WINDOW w_distinct_student_id_in_grade AS (PARTITION BY distinct_student_id_in_grade_status_course_id, DATE_TRUNC('MONTH', enrolled_at))
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    distinct_student_id_in_grade_status_course_id,
    score_cume_dist,
    grade_status_course_id_max_score_filled,
    distinct_student_id_in_course_id,
    distinct_course_id_in_distinct_student_id_in_gra,
    COALESCE(score_filled, 0.0) AS score_filled_filled,
    COALESCE(score_filled, 0.0) AS score_filled_filled_2
FROM metrics
ORDER BY distinct_student_id_in_course_id DESC, course_id DESC
