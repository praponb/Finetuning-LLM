WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        INITCAP(TRIM(cohort)) AS cohort_clean
    FROM students
    WHERE gpa > 2.5
      AND cohort <> 'C2023'
),
latest_per_key AS (
    -- Keep the latest row per major
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        cohort_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY major ORDER BY enrolled_on DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        cohort_clean,
        DATEDIFF(DATE '2025-06-30', enrolled_on) AS days_since_enrolled_on,
        DATE_FORMAT(enrolled_on, 'yyyy-MM') AS enrolled_on_label
    FROM latest_per_key
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        cohort,
        days_since_enrolled_on,
        enrolled_on_label,
        RANK() OVER (PARTITION BY cohort, major ORDER BY days_since_enrolled_on DESC) AS days_since_enrolled_on_rank,
        SUM(days_since_enrolled_on) OVER (PARTITION BY cohort, major ORDER BY days_since_enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_enrolled_on
    FROM tagged
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        cohort,
        days_since_enrolled_on,
        enrolled_on_label,
        days_since_enrolled_on_rank,
        running_days_since_enrolled_on,
        ROUND(days_since_enrolled_on / NULLIF(SUM(days_since_enrolled_on) OVER (PARTITION BY days_since_enrolled_on_rank, cohort), 0), 4) AS days_since_enrolled_on_share_of_days_since_enrol,
        SUM(days_since_enrolled_on) OVER (PARTITION BY days_since_enrolled_on_rank ORDER BY days_since_enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_enrolled_on_2
    FROM with_trends
),
latest_per_key_2 AS (
    -- Keep the earliest row per cohort
    SELECT
        id,
        cohort,
        days_since_enrolled_on,
        enrolled_on_label,
        days_since_enrolled_on_rank,
        running_days_since_enrolled_on,
        days_since_enrolled_on_share_of_days_since_enrol,
        running_days_since_enrolled_on_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY running_days_since_enrolled_on_2, id) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    COALESCE(cohort, 'ALL') AS cohort,
    days_since_enrolled_on_rank,
    ARRAY_SORT(COLLECT_SET(cohort)) AS cohort_list,
    PERCENTILE_APPROX(days_since_enrolled_on_share_of_days_since_enrol, 0.5) AS median_days_since_enrolled_on_share_of_days_sinc,
    COUNT(DISTINCT cohort) AS distinct_cohorts
FROM latest_per_key_2
GROUP BY GROUPING SETS ((cohort, days_since_enrolled_on_rank), (cohort), ())
ORDER BY cohort NULLS LAST, days_since_enrolled_on_rank NULLS LAST
