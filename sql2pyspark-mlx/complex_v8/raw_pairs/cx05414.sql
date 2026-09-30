WITH prepared AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        gpa,
        is_international,
        CASE WHEN is_international THEN 1 ELSE 0 END AS is_international_flag
    FROM students
    WHERE major = 'Art'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(gpa * 1.07, 2) AS gpa_adj,
        CASE WHEN gpa >= 3.4 THEN 'HIGH' WHEN gpa >= 2.7 THEN 'MEDIUM' ELSE 'LOW' END AS gpa_tier,
        INITCAP(TRIM(student_name)) AS student_name_clean
    FROM prepared
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN is_international THEN 1 ELSE 0 END AS is_international_flag_2,
        LOWER(REGEXP_REPLACE(TRIM(student_name_clean), ' ', '_')) AS student_name_clean_slug
    FROM labeled
    WHERE cohort <> 'C2021'
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(major, 'ALL') AS major,
    is_international,
    COALESCE(cohort, 'ALL') AS cohort,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(major)) AS major_list,
    ARRAY_SORT(COLLECT_SET(cohort)) AS cohort_list
FROM tagged
GROUP BY GROUPING SETS ((major, is_international, cohort), (major), (is_international), ())
ORDER BY major NULLS LAST, is_international NULLS LAST, cohort NULLS LAST
