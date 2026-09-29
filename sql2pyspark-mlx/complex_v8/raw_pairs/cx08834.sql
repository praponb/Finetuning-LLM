WITH valid AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        is_international,
        ABS(gpa - 2.7) AS gpa_distance,
        DATE_TRUNC('WEEK', enrolled_on) AS enrolled_on_week
    FROM students
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_on) AS enrolled_on_month,
        is_international,
        cohort,
        COUNT(DISTINCT cohort) AS distinct_cohorts,
        MAX(enrolled_on) AS last_enrolled_on
    FROM valid
    WHERE gpa >= 2.0
    GROUP BY 1, 2, 3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(distinct_cohorts - AVG(distinct_cohorts) OVER (PARTITION BY is_international, distinct_cohorts), 2) AS distinct_cohorts_vs_is_international_distinct_co,
        ROUND(distinct_cohorts / NULLIF(SUM(distinct_cohorts) OVER (PARTITION BY is_international, distinct_cohorts), 0), 4) AS distinct_cohorts_share_of_is_international_disti,
        ROUND(PERCENT_RANK() OVER w_is_international_by_distinct, 3) AS distinct_cohorts_percent_rank
    FROM totals
    WINDOW w_is_international_distinct_co AS (PARTITION BY is_international, distinct_cohorts), w_is_international_by_distinct AS (PARTITION BY is_international ORDER BY distinct_cohorts ASC)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN cohort IN ('C2021', 'C2024') THEN 'group_a' WHEN cohort = 'C2023' THEN 'group_b' ELSE 'other' END AS cohort_group
    FROM with_windows
)
-- Native SQL UNPIVOT (wide to long)
SELECT enrolled_on_month, is_international, metric, value
FROM (
    SELECT enrolled_on_month, is_international, distinct_cohorts_percent_rank, distinct_cohorts_share_of_is_international_disti, distinct_cohorts_vs_is_international_distinct_co
    FROM labeled
) src
UNPIVOT (
    value FOR metric IN (distinct_cohorts_percent_rank, distinct_cohorts_share_of_is_international_disti, distinct_cohorts_vs_is_international_distinct_co)
) unp
