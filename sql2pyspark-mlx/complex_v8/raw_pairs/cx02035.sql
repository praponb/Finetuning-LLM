WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(term)) AS term_clean,
        FLOOR(score / 25) * 25 AS score_bucket,
        UPPER(TRIM(term)) AS term_clean_2
    FROM enrollments
    WHERE term = '2025T2'
      AND enrolled_at < TIMESTAMP '2025-03-08 11:00:00'
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        term_clean,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        CONCAT_WS(' / ', term_clean_2, term) AS term_clean_2_term_label,
        SUBSTRING(TRIM(term_clean), 1, 4) AS term_clean_prefix
    FROM prepared_enrollments
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        course_id,
        score,
        enrolled_at,
        term_clean,
        grade_status_len,
        term_clean_2_term_label,
        term_clean_prefix,
        SUM(grade_status_len) OVER w_term AS term_sum_grade_status_len,
        ROUND(CUME_DIST() OVER (PARTITION BY term ORDER BY score DESC), 3) AS score_cume_dist,
        ROUND(score - AVG(score) OVER (PARTITION BY term), 2) AS score_vs_term_avg,
        MIN(score) OVER w_term AS term_min_score,
        ROW_NUMBER() OVER (PARTITION BY term ORDER BY score DESC, id) AS seq_no
    FROM annotated
    WINDOW w_term AS (PARTITION BY term)
)
-- Final projection
SELECT
    id,
    course_id,
    enrolled_at,
    term_clean,
    term_clean_2_term_label,
    term_sum_grade_status_len,
    score_cume_dist,
    score_vs_term_avg,
    term_min_score,
    seq_no
FROM with_windows
ORDER BY score_vs_term_avg DESC
