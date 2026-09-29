WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        LENGTH(TRIM(term)) AS term_len,
        LENGTH(TRIM(term)) AS term_len_2
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 2)
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.enrolled_at < TIMESTAMP '2025-02-24 08:00:00'
        )
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        term_len,
        COUNT(*) OVER w_term_len AS rows_in_term_len,
        SIZE(COLLECT_SET(term) OVER w_term_len) AS distinct_term_in_term_len,
        MAX(term_len) OVER (PARTITION BY term_len) AS term_len_max_term_len
    FROM qualified
    WINDOW w_term_len AS (PARTITION BY term_len)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    term_len,
    rows_in_term_len,
    distinct_term_in_term_len,
    SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
FROM with_trends
