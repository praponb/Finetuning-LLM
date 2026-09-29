WITH scoped AS (
    -- Scope the students rows
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        CONCAT_WS(': ', major, student_name) AS major_student_name_label,
        CONCAT(cohort, '#', CAST(id AS STRING)) AS cohort_ref,
        UPPER(TRIM(major)) AS major_clean
    FROM students
    WHERE is_international = FALSE
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        major,
        cohort,
        ARRAY_SORT(COLLECT_SET(cohort)) AS cohort_list,
        SUM(CASE WHEN cohort IS NOT NULL THEN 1 ELSE 0 END) AS n_cohort_is_not_null,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT major) AS distinct_majors,
        COUNT(DISTINCT cohort) AS distinct_cohorts
    FROM scoped
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE n_cohort_is_not_null < (SELECT MAX(n_cohort_is_not_null) FROM grouped)
),
screened_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM screened
    WHERE n_cohort_is_not_null < (SELECT MAX(n_cohort_is_not_null) FROM screened)
),
derived AS (
    -- Derive helper columns
    SELECT
        major,
        cohort,
        cohort_list,
        n_cohort_is_not_null,
        n_rows,
        distinct_majors,
        LENGTH(TRIM(cohort)) AS cohort_len
    FROM screened_2
),
screened_3 AS (
    -- Compare against the overall min
    SELECT *
    FROM derived
    WHERE cohort_len > (SELECT MIN(cohort_len) FROM derived)
)
-- Final projection
SELECT
    major,
    cohort,
    cohort_list,
    n_cohort_is_not_null,
    cohort_len,
    SIZE(cohort_list) AS cohort_list_count
FROM screened_3
