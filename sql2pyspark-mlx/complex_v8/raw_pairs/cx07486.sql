WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label
    FROM enrollments
),
latest_per_key AS (
    -- Keep the earliest row per student_id, course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_label,
        grade_status_ref,
        term_grade_status_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at, id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    term,
    student_id,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list,
    SUM(score) AS total_score,
    ROUND(100 * COUNT(CASE WHEN term IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_term_is_not_null
FROM latest_per_key
GROUP BY CUBE(term, student_id)
ORDER BY term NULLS LAST, student_id NULLS LAST
