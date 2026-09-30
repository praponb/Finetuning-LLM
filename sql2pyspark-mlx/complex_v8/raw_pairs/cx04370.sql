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
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        grade_status_clean,
        DENSE_RANK() OVER (PARTITION BY term ORDER BY enrolled_at) AS enrolled_at_dense_rank,
        ROUND(PERCENT_RANK() OVER w_course_id_term_by_score_ties, 3) AS score_percent_rank,
        COUNT(*) OVER w_student_id AS rows_in_student_id,
        RANK() OVER (PARTITION BY course_id, term ORDER BY score DESC) AS score_rank,
        COUNT(*) OVER (PARTITION BY student_id, grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER (PARTITION BY term ORDER BY enrolled_at, id) AS seq_no,
        SIZE(COLLECT_SET(term) OVER w_student_id) AS distinct_term_in_student_id,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_id, grade_status ORDER BY score DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY course_id, term ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at
    FROM filtered
    WINDOW w_course_id_term_by_score_ties AS (PARTITION BY course_id, term ORDER BY score DESC), w_student_id AS (PARTITION BY student_id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        score,
        grade_status,
        grade_status_clean,
        score_percent_rank,
        rows_in_student_id,
        score_rank,
        seq_no,
        distinct_term_in_student_id,
        minutes_since_prev_enrolled_at,
        first_enrolled_at,
        ROUND(score_rank / NULLIF(score_percent_rank, 0), 3) AS score_rank_per_score_percent_rank,
        CONCAT_WS(' / ', grade_status_clean, grade_status) AS grade_status_clean_grade_status_label,
        CONCAT_WS('-', grade_status, grade_status_clean) AS grade_status_grade_status_clean_label
    FROM sequenced
)
-- Final projection
SELECT
    id,
    student_id,
    grade_status_clean,
    score_rank,
    distinct_term_in_student_id,
    minutes_since_prev_enrolled_at,
    score_rank_per_score_percent_rank,
    grade_status_clean_grade_status_label,
    grade_status_grade_status_clean_label,
    LOWER(TRIM(grade_status_clean)) AS grade_status_clean_clean,
    CASE WHEN grade_status IN ('FAIL', 'PASS') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group
FROM enriched
WHERE UPPER(TRIM(grade_status_clean)) LIKE 'I%'
ORDER BY grade_status_clean_grade_status_label DESC, grade_status_grade_status_clean_label, id
LIMIT 5
