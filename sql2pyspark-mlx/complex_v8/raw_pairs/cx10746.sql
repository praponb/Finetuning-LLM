WITH filtered AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        MONTH(enrolled_on) AS enrolled_on_month,
        DATEDIFF(DATE '2026-01-01', enrolled_on) AS days_since_enrolled_on,
        DATEDIFF(DATE '2026-01-01', enrolled_on) AS days_since_enrolled_on_2
    FROM students
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN gpa >= 3.2 THEN 'large' WHEN gpa >= 2.7 THEN 'medium' ELSE 'small' END AS gpa_tier
    FROM filtered
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.id) AS avg_enrollment_score,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.id) AS min_enrollment_score
    FROM scored s
),
grouped AS (
    -- Summarise per group
    SELECT
        cohort,
        major,
        PERCENTILE_APPROX(gpa, 0.5) AS median_gpa,
        COUNT(*) AS n_rows,
        ROUND(SUM(days_since_enrolled_on_2) / NULLIF(SUM(days_since_enrolled_on), 0), 3) AS days_since_enrolled_on_2_to_days_since_enrolled_,
        COUNT(DISTINCT gpa_tier) AS distinct_gpa_tiers
    FROM with_counts
    GROUP BY 1, 2
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE n_rows < (SELECT MAX(n_rows) FROM grouped)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE distinct_gpa_tiers > (SELECT AVG(distinct_gpa_tiers) FROM without_matches)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT major, n_rows, cohort, median_gpa
    FROM with_activity
) src
PIVOT (
    MIN(median_gpa) AS min_v, COUNT(median_gpa) AS n
    FOR cohort IN ('C2022', 'C2024', 'C2021', 'C2023')
)
ORDER BY major, n_rows
