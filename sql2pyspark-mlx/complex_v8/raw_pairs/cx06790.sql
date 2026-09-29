WITH prepared AS (
    -- Scope the students rows
    SELECT
        id,
        cohort,
        enrolled_on,
        gpa,
        is_international,
        CONCAT_WS(': ', cohort, major) AS cohort_major_label
    FROM students
    WHERE enrolled_on BETWEEN DATE '2024-03-15' AND DATE '2025-07-03'
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_on) AS enrolled_on_month,
        MAX(enrolled_on) AS last_enrolled_on,
        SUM(gpa) AS total_gpa,
        MIN(enrolled_on) AS first_enrolled_on
    FROM prepared
    GROUP BY 1
),
tagged AS (
    -- Add calculated columns
    SELECT *, DATE_TRUNC('YEAR', enrolled_on_month) AS enrolled_on_month_year
    FROM stats
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM tagged
    WHERE total_gpa > (SELECT MIN(total_gpa) FROM tagged)
)
-- Final projection
SELECT
    enrolled_on_month,
    last_enrolled_on,
    total_gpa,
    first_enrolled_on,
    COALESCE(total_gpa, 0.0) AS total_gpa_filled
FROM without_matches
ORDER BY enrolled_on_month DESC, first_enrolled_on DESC NULLS FIRST
LIMIT 5
