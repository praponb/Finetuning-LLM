WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score < 59.8
      AND term NOT IN ('2025T2', '2025T1')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY term)) AS distinct_student_id_in_term,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id, grade_status)) AS distinct_term_in_course_id_grade_status,
        LEAD(enrolled_at, 3) OVER w_enrolled_at_to_date_by_score AS next3_enrolled_at,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id, grade_status), 2) AS score_vs_course_id_grade_status_avg,
        ROW_NUMBER() OVER (PARTITION BY course_id, term ORDER BY enrolled_at DESC, id) AS enrolled_at_seq,
        MAX(score) OVER (PARTITION BY course_id, term ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        LEAD(enrolled_at) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id) AS next_enrolled_at,
        COUNT(*) OVER w_course_id_grade_status AS rows_in_course_id_grade_status,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        ROUND(PERCENT_RANK() OVER w_course_id_grade_status_by_en_2, 3) AS enrolled_at_percent_rank,
        ROUND(score / NULLIF(SUM(score) OVER w_term, 0), 4) AS score_share_of_term,
        ROUND(AVG(score) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_avg_1_prior,
        ROUND(AVG(score) OVER (PARTITION BY student_id ORDER BY enrolled_at ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_avg_2_prior,
        COUNT(*) OVER w_term AS rows_in_term
    FROM recent_enrollments
    WINDOW w_enrolled_at_to_date_by_score AS (PARTITION BY TO_DATE(enrolled_at) ORDER BY score, id), w_course_id_grade_status AS (PARTITION BY course_id, grade_status), w_course_id_grade_status_by_en_2 AS (PARTITION BY course_id, grade_status ORDER BY enrolled_at), w_term AS (PARTITION BY term)
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(score_avg_2_prior - AVG(score_avg_2_prior) OVER (PARTITION BY grade_status, course_id), 2) AS score_avg_2_prior_vs_grade_status_course_id_avg,
        MAX(score_avg_2_prior) OVER w_grade_status_course_id AS grade_status_course_id_max_score_avg_2_prior,
        LAST_VALUE(enrolled_at_percent_rank) IGNORE NULLS OVER (PARTITION BY term ORDER BY score_share_of_term, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY term ORDER BY score_share_of_term, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        LAST_VALUE(score_avg_2_prior) IGNORE NULLS OVER (PARTITION BY term ORDER BY score_share_of_term, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_avg_2_prior
    FROM metrics
    WINDOW w_grade_status_course_id AS (PARTITION BY grade_status, course_id)
)
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    distinct_student_id_in_term,
    next3_enrolled_at,
    score_vs_course_id_grade_status_avg,
    enrolled_at_seq,
    max_score_so_far,
    rows_in_course_id_grade_status,
    last_known_score,
    enrolled_at_percent_rank,
    score_share_of_term,
    score_avg_1_prior,
    score_avg_2_prior,
    rows_in_term,
    score_avg_2_prior_vs_grade_status_course_id_avg,
    grade_status_course_id_max_score_avg_2_prior,
    last_known_enrolled_at_percent_rank,
    last_known_score_avg_2_prior
FROM metrics_2
ORDER BY last_known_enrolled_at_percent_rank, score_avg_2_prior_vs_grade_status_course_id_avg DESC
