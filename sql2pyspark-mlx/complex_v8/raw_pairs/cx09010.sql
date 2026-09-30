WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        cohort,
        major,
        gpa,
        is_international,
        IF(is_international, 'yes', 'no') AS is_international_label
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'D%'
),
latest AS (
    -- Keep the latest row per cohort
    SELECT
        id,
        cohort,
        major,
        gpa,
        is_international,
        is_international_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY gpa DESC, id DESC) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
totals AS (
    -- Summarise per group
    SELECT
        major,
        cohort,
        SUM(gpa) FILTER (WHERE cohort = 'C2024') AS gpa_cohort_c2024,
        ARRAY_SORT(COLLECT_SET(major)) AS major_list,
        COUNT(DISTINCT major) AS distinct_majors,
        COUNT(DISTINCT cohort) AS distinct_cohorts,
        ROUND(STDDEV(gpa), 2) AS stddev_gpa
    FROM latest
    GROUP BY major, cohort
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        major,
        cohort,
        gpa_cohort_c2024,
        major_list,
        stddev_gpa,
        SIZE(COLLECT_SET(major) OVER (PARTITION BY cohort, distinct_cohorts)) AS distinct_major_in_cohort_distinct_cohorts,
        NTH_VALUE(gpa_cohort_c2024, 2) OVER (ORDER BY stddev_gpa DESC, major, cohort ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa_cohort_c2024,
        FIRST_VALUE(distinct_cohorts) OVER (ORDER BY stddev_gpa DESC, major, cohort ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_cohorts
    FROM totals
)
SELECT
    major,
    cohort,
    gpa_cohort_c2024,
    major_list,
    stddev_gpa,
    second_gpa_cohort_c2024,
    first_distinct_cohorts
FROM windowed
WHERE gpa_cohort_c2024 <= 13.0
ORDER BY major, stddev_gpa, cohort
LIMIT 10
