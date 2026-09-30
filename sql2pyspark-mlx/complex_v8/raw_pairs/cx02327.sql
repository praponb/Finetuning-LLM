WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS('|', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE term IN ('2024T2', '2025T1')
      AND score <= 74.9
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(score) OVER (PARTITION BY grade_status, student_id) AS grade_status_student_id_max_score,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, student_id), 2) AS grade_status_student_id_avg_score,
        ROUND(CUME_DIST() OVER w_all_by_score_ties, 3) AS score_cume_dist,
        ROUND(PERCENT_RANK() OVER w_student_id_term_by_score, 3) AS score_percent_rank,
        NTILE(4) OVER w_grade_status_term_by_enrolle AS enrolled_at_quartile,
        SUM(score) OVER (PARTITION BY grade_status, course_id) AS grade_status_course_id_sum_score,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, course_id), 2) AS score_vs_grade_status_course_id_avg,
        ROUND(AVG(score) OVER (PARTITION BY student_id, term ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2,
        LAST_VALUE(score) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, course_id ORDER BY score), 3) AS score_percent_rank_2,
        ROW_NUMBER() OVER (ORDER BY score DESC, id) AS rn,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY grade_status, student_id)) AS distinct_term_in_grade_status_student_id,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status, student_id), 0), 4) AS score_share_of_grade_status_student_id,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_grade_status_term_by_enrolle)) / 60, 1) AS minutes_since_prev_enrolled_at,
        LEAD(enrolled_at) OVER w_all_by_score_2 AS next_enrolled_at
    FROM scoped_enrollments
    WINDOW w_all_by_score_ties AS (ORDER BY score DESC NULLS FIRST), w_grade_status_student_id AS (PARTITION BY grade_status, student_id), w_student_id_term_by_score AS (PARTITION BY student_id, term ORDER BY score, id), w_grade_status_term_by_enrolle AS (PARTITION BY grade_status, term ORDER BY enrolled_at ASC, id), w_all_by_score_2 AS (ORDER BY score DESC NULLS FIRST, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE rn = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(score_cume_dist) OVER (ORDER BY score_share_of_grade_status_student_id DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_cume_dist_sum_3,
        ROUND(score - AVG(score) OVER (PARTITION BY term), 2) AS score_vs_term_avg,
        LAST_VALUE(enrolled_at_quartile) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), enrolled_at_quartile ORDER BY grade_status_student_id_avg_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at_quartile
    FROM top_rows
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    term_grade_status_label,
    grade_status_student_id_max_score,
    grade_status_student_id_avg_score,
    score_percent_rank,
    enrolled_at_quartile,
    grade_status_course_id_sum_score,
    score_avg_2,
    score_percent_rank_2,
    rn,
    distinct_term_in_grade_status_student_id,
    score_share_of_grade_status_student_id,
    minutes_since_prev_enrolled_at,
    next_enrolled_at,
    score_vs_term_avg,
    last_known_enrolled_at_quartile,
    rn - score_cume_dist_sum_3 AS rn_minus_score_cume_dist_sum_3
FROM windowed
ORDER BY grade_status DESC NULLS FIRST, rn
