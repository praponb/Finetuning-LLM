WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE term IN ('2024T1', '2025T2')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        term_grade_status_label,
        LEAD(enrolled_at) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS next_enrolled_at,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY grade_status)) AS distinct_term_in_grade_status,
        MAX(score) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_max_3,
        MIN(score) OVER (PARTITION BY grade_status) AS grade_status_min_score,
        ROW_NUMBER() OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS seq_no,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at DESC NULLS FIRST, id), 3) AS enrolled_at_cume_dist
    FROM prepared
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT term, grade_status, score_max_3
    FROM with_trends
) src
PIVOT (
    COUNT(score_max_3) AS count_v, SUM(score_max_3) AS sum_v
    FOR grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'PASS', 'FAIL')
)
ORDER BY term
