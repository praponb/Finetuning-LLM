WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE score BETWEEN 34.2 AND 94.2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        grade_status,
        enrolled_at,
        grade_status_term_label,
        grade_status_ref,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROUND(score - AVG(score) OVER w_term, 2) AS score_vs_term_avg,
        ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id) AS seq_no,
        NTILE(10) OVER (ORDER BY enrolled_at DESC, id) AS enrolled_at_decile,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY score, id) AS score_dense_rank,
        SUM(score) OVER (PARTITION BY course_id ORDER BY score, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_sum_7,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY enrolled_at DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        RANK() OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id) AS enrolled_at_rank,
        ROW_NUMBER() OVER w_all_by_enrolled_at AS enrolled_at_seq,
        ROUND(AVG(score) OVER (PARTITION BY term, MONTH(enrolled_at) ORDER BY score DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2,
        score - LAG(score) OVER w_term_enrolled_at_month_by_sc_2 AS score_change,
        ROUND(AVG(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_avg_2_prior,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at DESC), 3) AS enrolled_at_cume_dist
    FROM filtered
    WINDOW w_term AS (PARTITION BY term), w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC, id), w_term_enrolled_at_month_by_sc_2 AS (PARTITION BY term, MONTH(enrolled_at) ORDER BY score DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(grade_status_term_label)) AS grade_status_term_label_clean
    FROM windowed
)
SELECT
    id,
    student_id,
    grade_status,
    grade_status_term_label,
    grade_status_ref,
    first_known_enrolled_at,
    score_vs_term_avg,
    seq_no,
    enrolled_at_decile,
    score_dense_rank,
    score_avg_2,
    score_change,
    score_avg_2_prior,
    grade_status_term_label_clean,
    LENGTH(TRIM(grade_status)) AS grade_status_len
FROM labeled
