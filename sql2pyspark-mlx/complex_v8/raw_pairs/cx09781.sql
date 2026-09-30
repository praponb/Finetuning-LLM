WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m,
        INITCAP(TRIM(term)) AS term_clean,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
),
unique_rows AS (
    -- Keep the latest row per course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        enrolled_at_plus_30m,
        term_clean,
        grade_status_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY grade_status_len DESC, id DESC) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(term_clean)) AS term_clean_len,
        HOUR(enrolled_at) AS enrolled_at_hour,
        ABS(grade_status_len - 7) AS grade_status_len_distance
    FROM unique_rows
)
-- Subtotals with ROLLUP
SELECT
    grade_status,
    student_id,
    term,
    GROUPING(grade_status) AS is_total_grade_status,
    SUM(enrolled_at_hour) AS total_enrolled_at_hour,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list
FROM derived
GROUP BY ROLLUP(grade_status, student_id, term)
ORDER BY grade_status NULLS LAST, student_id NULLS LAST, term NULLS LAST
