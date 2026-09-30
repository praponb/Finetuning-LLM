WITH valid_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        cohort,
        gpa,
        is_international,
        SUBSTRING(TRIM(major), 1, 4) AS major_prefix,
        LOWER(REGEXP_REPLACE(TRIM(cohort), ' ', '_')) AS cohort_slug,
        INITCAP(TRIM(cohort)) AS cohort_clean
    FROM students
    WHERE gpa < 3.3
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS last_enrollment_enrolled_at
    FROM valid_students s
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(gpa) OVER (PARTITION BY is_international, cohort) AS is_international_cohort_min_gpa,
        ROUND(CUME_DIST() OVER (PARTITION BY cohort ORDER BY gpa DESC), 3) AS gpa_cume_dist
    FROM with_counts
)
SELECT
    cohort,
    is_international,
    ROUND(AVG(gpa_cume_dist), 2) AS avg_gpa_cume_dist,
    ROUND(100 * COUNT(CASE WHEN cohort NOT IN ('C2021', 'C2023') THEN 1 END) / COUNT(*), 1) AS pct_cohort_not_in_c2021_c2023,
    MAX(gpa_cume_dist) AS max_gpa_cume_dist
FROM metrics
GROUP BY GROUPING SETS ((cohort, is_international), (cohort), (is_international), ())
ORDER BY cohort NULLS LAST, is_international NULLS LAST
