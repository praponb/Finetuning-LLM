WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 62.5) AS score_distance,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix
    FROM enrollments
    WHERE course_id >= 11
      AND student_id <= 7
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        term,
        score,
        score_distance,
        term_prefix,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id), 3) AS enrolled_at_cume_dist,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id, grade_status), 0), 4) AS score_share_of_student_id_grade_status,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_id, grade_status)) AS distinct_term_in_student_id_grade_status,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        score_distance - LAG(score_distance) OVER w_grade_status_by_enrolled_at_2 AS score_distance_change,
        NTH_VALUE(score_distance, 2) OVER (PARTITION BY course_id, student_id ORDER BY score_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_distance,
        ROW_NUMBER() OVER w_grade_status_by_enrolled_at_2 AS enrolled_at_seq,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY student_id, grade_status)) AS distinct_course_id_in_student_id_grade_status,
        SUM(score) OVER (PARTITION BY student_id, grade_status) AS student_id_grade_status_sum_score,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER w_student_id_grade_status, 0), 4) AS score_distance_share_of_student_id_grade_status,
        ROUND(AVG(score) OVER (PARTITION BY student_id, grade_status), 2) AS student_id_grade_status_avg_score,
        ROW_NUMBER() OVER w_all_by_score AS seq_no
    FROM prepared
    WINDOW w_grade_status_by_enrolled_at_2 AS (PARTITION BY grade_status ORDER BY enrolled_at, id), w_student_id_grade_status AS (PARTITION BY student_id, grade_status), w_all_by_score AS (ORDER BY score, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        term,
        score,
        score_distance,
        term_prefix,
        enrolled_at_cume_dist,
        score_share_of_student_id_grade_status,
        second_enrolled_at,
        distinct_term_in_student_id_grade_status,
        first_enrolled_at,
        score_distance_change,
        second_score_distance,
        distinct_course_id_in_student_id_grade_status,
        student_id_grade_status_sum_score,
        score_distance_share_of_student_id_grade_status,
        student_id_grade_status_avg_score,
        seq_no
    FROM with_windows
    WHERE enrolled_at_seq = 1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        term,
        score,
        term_prefix,
        enrolled_at_cume_dist,
        score_share_of_student_id_grade_status,
        second_enrolled_at,
        distinct_term_in_student_id_grade_status,
        first_enrolled_at,
        score_distance_change,
        second_score_distance,
        distinct_course_id_in_student_id_grade_status,
        student_id_grade_status_sum_score,
        score_distance_share_of_student_id_grade_status,
        student_id_grade_status_avg_score,
        seq_no,
        COUNT(*) OVER w_term AS rows_in_term,
        NTH_VALUE(second_score_distance, 2) OVER (PARTITION BY distinct_term_in_student_id_grade_status ORDER BY second_enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_score_distance,
        ROUND(AVG(score) OVER (PARTITION BY term), 2) AS term_avg_score,
        COUNT(*) OVER (PARTITION BY distinct_term_in_student_id_grade_status ORDER BY second_enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(enrolled_at_cume_dist) OVER (PARTITION BY distinct_term_in_student_id_grade_status ORDER BY score_distance_share_of_student_id_grade_status DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_enrolled_at_cume_dist_so_far
    FROM leaders
    WINDOW w_term AS (PARTITION BY term)
)
-- Final projection
SELECT
    id,
    score,
    enrolled_at_cume_dist,
    score_share_of_student_id_grade_status,
    second_enrolled_at,
    first_enrolled_at,
    score_distance_change,
    second_score_distance,
    distinct_course_id_in_student_id_grade_status,
    student_id_grade_status_sum_score,
    score_distance_share_of_student_id_grade_status,
    seq_no,
    term_avg_score,
    running_count,
    min_enrolled_at_cume_dist_so_far,
    CASE WHEN term IN ('2024T1', '2025T1') THEN 'group_a' WHEN term = '2024T2' THEN 'group_b' ELSE 'other' END AS term_group,
    SUBSTRING(TRIM(term_prefix), 1, 4) AS term_prefix_prefix
FROM sequenced
WHERE distinct_course_id_in_student_id_grade_status > 2
