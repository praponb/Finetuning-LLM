WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        DATE_TRUNC('WEEK', enrolled_on) AS enrolled_on_week,
        COALESCE(cohort, 'UNKNOWN') AS cohort_or_unknown,
        DATE_TRUNC('YEAR', enrolled_on) AS enrolled_on_year
    FROM students
    WHERE gpa < 3.2
),
latest AS (
    -- Keep the latest row per major, cohort_or_unknown
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        enrolled_on_week,
        cohort_or_unknown,
        enrolled_on_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY major, cohort_or_unknown ORDER BY gpa DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
latest_2 AS (
    -- Keep the earliest row per cohort_or_unknown
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        enrolled_on_week,
        cohort_or_unknown,
        enrolled_on_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cohort_or_unknown ORDER BY enrolled_on_year, id) AS row_num
        FROM latest
    ) ranked
    WHERE row_num = 1
),
cohort_or_unknown_stats AS (
    -- Avg gpa per cohort_or_unknown
    SELECT cohort_or_unknown, AVG(gpa) AS avg_gpa
    FROM latest_2
    GROUP BY cohort_or_unknown
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_gpa, ROUND(s.gpa - g.avg_gpa, 2) AS gpa_minus_avg
    FROM latest_2 s
    JOIN cohort_or_unknown_stats g ON g.cohort_or_unknown = s.cohort_or_unknown
    WHERE s.gpa > g.avg_gpa
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', enrolled_on_year) AS enrolled_on_year_month,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(avg_gpa) / NULLIF(SUM(gpa_minus_avg), 0), 3) AS avg_gpa_to_gpa_minus_avg_ratio,
        SUM(avg_gpa) AS total_avg_gpa,
        COUNT(*) AS row_count,
        MIN(gpa_minus_avg) AS min_gpa_minus_avg
    FROM vs_group
    GROUP BY 1
)
SELECT
    enrolled_on_year_month,
    distinct_ids,
    avg_gpa_to_gpa_minus_avg_ratio,
    total_avg_gpa,
    row_count,
    min_gpa_minus_avg
FROM stats
ORDER BY total_avg_gpa DESC NULLS FIRST
