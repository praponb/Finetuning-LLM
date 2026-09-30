WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 1 DAYS AS enrolled_at_plus_1d
    FROM enrollments
    WHERE grade_status IN ('INCOMPLETE', 'PASS')
      AND TO_DATE(enrolled_at) < DATE '2025-02-18'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(score) OVER (PARTITION BY grade_status ORDER BY enrolled_at_plus_1d ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at_plus_1d ASC) AS enrolled_at_plus_1d_rank,
        ROW_NUMBER() OVER (ORDER BY enrolled_at ASC, id) AS seq_no,
        COUNT(*) OVER w_course_id AS rows_in_course_id,
        LEAD(enrolled_at_plus_1d) OVER (PARTITION BY course_id, student_id ORDER BY enrolled_at DESC, id) AS next_enrolled_at_plus_1d,
        SUM(score) OVER (PARTITION BY grade_status) AS grade_status_sum_score,
        ROUND(score / NULLIF(SUM(score) OVER w_course_id, 0), 4) AS score_share_of_course_id,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        DENSE_RANK() OVER w_enrolled_at_plus_1d_month_by AS score_dense_rank,
        MAX(score) OVER (PARTITION BY grade_status ORDER BY enrolled_at_plus_1d ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_max_4,
        LAG(enrolled_at_plus_1d, 2) OVER w_enrolled_at_plus_1d_to_date AS prev2_enrolled_at_plus_1d,
        RANK() OVER (PARTITION BY TO_DATE(enrolled_at_plus_1d) ORDER BY enrolled_at) AS enrolled_at_rank,
        ROUND(score / NULLIF(SUM(score) OVER w_grade_status, 0), 4) AS score_share_of_grade_status,
        NTH_VALUE(enrolled_at_plus_1d, 2) OVER (PARTITION BY TO_DATE(enrolled_at_plus_1d) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_plus_1d,
        FIRST_VALUE(score) IGNORE NULLS OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score
    FROM filtered_enrollments
    WINDOW w_course_id AS (PARTITION BY course_id), w_enrolled_at_plus_1d_month_by AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_plus_1d) ORDER BY score, id), w_enrolled_at_plus_1d_to_date AS (PARTITION BY TO_DATE(enrolled_at_plus_1d) ORDER BY enrolled_at, id), w_grade_status AS (PARTITION BY grade_status)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE score_dense_rank = 1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY enrolled_at_rank, student_id)) AS distinct_course_id_in_enrolled_at_rank_student_i,
        ROUND(CUME_DIST() OVER (ORDER BY first_enrolled_at), 3) AS first_enrolled_at_cume_dist,
        MAX(score_avg_3_prior) OVER (PARTITION BY score_dense_rank, MONTH(enrolled_at)) AS score_dense_rank_enrolled_at_month_max_score_avg,
        LEAD(first_enrolled_at) OVER w_seq_no_enrolled_at_plus_1d_m AS next_first_enrolled_at,
        LAST_VALUE(score_max_4) OVER (ORDER BY first_enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_max_4,
        RANK() OVER (ORDER BY score_share_of_course_id NULLS LAST) AS score_share_of_course_id_rank
    FROM top_rows
    WINDOW w_seq_no_enrolled_at_plus_1d_m AS (PARTITION BY seq_no, MONTH(enrolled_at_plus_1d) ORDER BY enrolled_at_plus_1d DESC NULLS FIRST, id)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    enrolled_at_plus_1d,
    score_avg_3_prior,
    enrolled_at_plus_1d_rank,
    seq_no,
    rows_in_course_id,
    next_enrolled_at_plus_1d,
    grade_status_sum_score,
    first_enrolled_at,
    score_dense_rank,
    score_max_4,
    prev2_enrolled_at_plus_1d,
    score_share_of_grade_status,
    first_known_score,
    distinct_course_id_in_enrolled_at_rank_student_i,
    first_enrolled_at_cume_dist,
    score_dense_rank_enrolled_at_month_max_score_avg,
    next_first_enrolled_at,
    score_share_of_course_id_rank,
    LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
FROM sequenced
WHERE last_score_max_4 < 255.0
ORDER BY score_avg_3_prior NULLS LAST, term DESC
