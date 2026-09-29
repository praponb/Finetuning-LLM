WITH filtered AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE LENGTH(TRIM(student_name)) >= 5
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE gpa > (SELECT AVG(gpa) FROM filtered)
),
per_group AS (
    -- Group-level totals
    SELECT
        major,
        COUNT(*) FILTER (WHERE cohort IN ('C2022', 'C2024')) AS n_cohort_in_c2022_c2024,
        SUM(CASE WHEN is_international = TRUE THEN 1 ELSE 0 END) AS n_is_international_true,
        ROUND(100 * COUNT(CASE WHEN cohort = 'C2021' THEN 1 END) / COUNT(*), 1) AS pct_cohort_c2021
    FROM without_matches
    WHERE cohort = 'C2023'
    GROUP BY major
    HAVING MAX(gpa) > 2.1
)
-- Final projection
SELECT
    major,
    n_is_international_true,
    pct_cohort_c2021,
    UPPER(TRIM(major)) AS major_clean,
    FLOOR(n_cohort_in_c2022_c2024 / 2) * 2 AS n_cohort_in_c2022_c2024_bucket
FROM per_group
ORDER BY major, major_clean DESC NULLS FIRST
LIMIT 20
