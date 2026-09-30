WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter,
        CASE WHEN term IN ('2024T2', '2025T2') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group,
        LOWER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE term = '2024T1'
      AND student_id IS NOT NULL
),
tagged AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM base
    WHERE grade_status <> 'INCOMPLETE'
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at_quarter,
        grade_status_clean,
        COALESCE(days_since_enrolled_at, 0) AS days_since_enrolled_at_filled,
        LENGTH(TRIM(term)) AS term_len
    FROM tagged
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    enrolled_at_quarter,
    grade_status_clean,
    days_since_enrolled_at_filled,
    term_len
FROM enriched
WHERE grade_status <> 'PASS'
ORDER BY term_len
