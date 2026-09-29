WITH valid AS (
    -- Scope the students rows
    SELECT
        id,
        cohort,
        enrolled_on,
        gpa,
        CONCAT_WS('-', major, cohort) AS major_cohort_label,
        LOWER(REGEXP_REPLACE(TRIM(major), ' ', '_')) AS major_slug
    FROM students
    WHERE major NOT IN ('Bio', 'Biz')
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(major_cohort_label, '#', CAST(id AS STRING)) AS major_cohort_label_ref
    FROM valid
    WHERE enrolled_on < DATE '2025-05-07'
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', enrolled_on) AS enrolled_on_month,
        cohort,
        MAX(enrolled_on) AS last_enrolled_on,
        COUNT(DISTINCT cohort) AS distinct_cohorts,
        PERCENTILE_APPROX(gpa, 0.5) AS median_gpa
    FROM derived
    WHERE gpa < 3.3
    GROUP BY DATE_TRUNC('MONTH', enrolled_on), cohort
    HAVING MAX(gpa) > 2.1
)
SELECT *
FROM (
    SELECT enrolled_on_month, distinct_cohorts, cohort, median_gpa
    FROM summary
) src
PIVOT (
    MIN(median_gpa)
    FOR cohort IN ('C2022', 'C2024', 'C2023', 'C2021')
)
ORDER BY enrolled_on_month, distinct_cohorts
