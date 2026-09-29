WITH recent AS (
    -- Scope the students rows
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        CASE WHEN is_international THEN 1 ELSE 0 END AS is_international_flag
    FROM students
    WHERE gpa >= 2.1
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', enrolled_on) AS enrolled_on_quarter,
        SUBSTRING(TRIM(major), 1, 4) AS major_prefix
    FROM recent
),
tagged AS (
    -- Add calculated columns
    SELECT *, COALESCE(cohort, 'UNKNOWN') AS cohort_or_unknown
    FROM annotated
)
-- Final projection
SELECT DISTINCT cohort, gpa, major_prefix
FROM tagged
ORDER BY cohort, gpa DESC NULLS FIRST
