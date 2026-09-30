WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND course_id IS NOT NULL
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared
    WHERE term_len > (SELECT AVG(term_len) FROM prepared)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        term_len,
        grade_status,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        COUNT(*) FILTER (WHERE grade_status = 'WITHDRAWN') AS n_grade_status_withdrawn,
        COUNT(*) AS cnt,
        ROUND(SUM(term_len) / NULLIF(SUM(score), 0), 3) AS term_len_to_score_ratio,
        COUNT(DISTINCT grade_status) AS distinct_grade_status
    FROM with_activity
    GROUP BY term_len, grade_status
)
-- Final projection
SELECT
    term_len,
    grade_status,
    grade_status_list,
    n_grade_status_withdrawn,
    cnt,
    term_len_to_score_ratio,
    distinct_grade_status,
    CONCAT(grade_status, '#', CAST(n_grade_status_withdrawn AS STRING)) AS grade_status_ref
FROM summary
ORDER BY grade_status_ref DESC NULLS FIRST
