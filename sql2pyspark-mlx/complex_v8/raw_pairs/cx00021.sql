WITH scoped AS (
    -- Filter students
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        DATE_TRUNC('WEEK', enrolled_on) AS enrolled_on_week,
        CONCAT(major, '#', CAST(id AS STRING)) AS major_ref,
        CASE WHEN cohort IN ('C2023', 'C2022') THEN 'group_a' WHEN cohort = 'C2024' THEN 'group_b' ELSE 'other' END AS cohort_group
    FROM students
    WHERE cohort NOT IN ('C2022', 'C2024')
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('DAY', enrolled_on_week) AS enrolled_on_week_day,
        YEAR(enrolled_on_week) AS enrolled_on_week_year
    FROM scoped
)
SELECT
    enrolled_on_week_year,
    cohort,
    ARRAY_SORT(COLLECT_SET(cohort)) AS cohort_list
FROM labeled
GROUP BY GROUPING SETS ((enrolled_on_week_year, cohort), (enrolled_on_week_year))
ORDER BY enrolled_on_week_year NULLS LAST, cohort NULLS LAST
