WITH cleaned AS (
    -- Filter students
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        LENGTH(TRIM(major)) AS major_len,
        LAST_DAY(enrolled_on) AS enrolled_on_month_end
    FROM students
    WHERE NOT is_international
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.term <> '2025T2'
        )
),
per_group AS (
    -- Summarise per group
    SELECT
        major,
        DATE_TRUNC('MONTH', enrolled_on) AS enrolled_on_month,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM filtered
    GROUP BY major, DATE_TRUNC('MONTH', enrolled_on)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(enrolled_on_month, 'yyyy-MM-dd') AS enrolled_on_month_label,
        CASE WHEN row_count >= 8 THEN 'A' WHEN row_count >= 6 THEN 'B' ELSE 'C' END AS row_count_tier
    FROM per_group
),
enriched AS (
    -- Derive helper columns
    SELECT *, TO_DATE(enrolled_on_month) AS enrolled_on_month_date
    FROM labeled
)
SELECT
    major,
    enrolled_on_month,
    distinct_ids,
    enrolled_on_month_label,
    row_count_tier,
    enrolled_on_month_date
FROM enriched
WHERE major IS NOT NULL
ORDER BY distinct_ids DESC
