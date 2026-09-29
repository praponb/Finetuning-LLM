WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE student_id >= 4
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(score) OVER (PARTITION BY YEAR(enrolled_at), course_id ORDER BY score, id) AS prev_score,
        ROW_NUMBER() OVER (PARTITION BY YEAR(enrolled_at), course_id ORDER BY score, id) AS seq_no,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        RANK() OVER w_course_id_grade_status_by_en AS enrolled_at_rank,
        NTILE(10) OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC, id) AS enrolled_at_decile,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_id, DATE_TRUNC('MONTH', enrolled_at) ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        NTH_VALUE(score, 2) OVER (PARTITION BY YEAR(enrolled_at), course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        COUNT(*) OVER (PARTITION BY YEAR(enrolled_at), course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(score) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_sum_3_prior
    FROM scoped
    WINDOW w_enrolled_at_year_course_id_b AS (PARTITION BY YEAR(enrolled_at), course_id ORDER BY score, id), w_course_id_grade_status_by_en AS (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(score_sum_3_prior - 180.0) AS score_sum_3_prior_distance,
        CONCAT_WS(': ', grade_status, term_grade_status_label) AS grade_status_term_grade_status_label_label,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM metrics
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    grade_status,
    enrolled_at,
    term_grade_status_label,
    prev_score,
    seq_no,
    enrolled_at_rank,
    enrolled_at_decile,
    first_known_score,
    second_score,
    running_count,
    score_sum_3_prior,
    grade_status_or_unknown,
    ABS(running_count - 8) AS running_count_distance,
    ROUND(first_known_score / NULLIF(running_count, 0), 3) AS first_known_score_per_running_count
FROM scored
ORDER BY grade_status, term_grade_status_label NULLS LAST
