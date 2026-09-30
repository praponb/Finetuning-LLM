WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE course_id >= 4
      AND enrolled_at < TIMESTAMP '2025-03-02 08:00:00'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        grade_status_slug,
        grade_status_len,
        COUNT(*) OVER (PARTITION BY grade_status_len, grade_status) AS rows_in_grade_status_len_grade_status,
        ROW_NUMBER() OVER (ORDER BY score DESC NULLS FIRST, id) AS score_seq
    FROM recent
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        enrolled_at,
        grade_status_len,
        rows_in_grade_status_len_grade_status,
        score_seq,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label,
        LOWER(REGEXP_REPLACE(TRIM(grade_status_slug), ' ', '_')) AS grade_status_slug_slug
    FROM metrics
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        enrolled_at,
        grade_status_len,
        rows_in_grade_status_len_grade_status,
        enrolled_at_label,
        grade_status_slug_slug
    FROM derived
    WHERE score_seq <= 2
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, grade_status_len, rows_in_grade_status_len_grade_status
    FROM top_rows
) src
UNPIVOT (
    value FOR field IN (grade_status_len AS m_grade_status_len, rows_in_grade_status_len_grade_status AS m_rows_in_grade_status_len_grade_status)
) unp
ORDER BY id, field
