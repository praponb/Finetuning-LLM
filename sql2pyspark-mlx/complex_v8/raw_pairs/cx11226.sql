WITH prepared_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        ABS(gpa - 2.9) AS gpa_distance
    FROM students
    WHERE cohort = 'C2024'
      AND UPPER(TRIM(student_name)) LIKE 'B%'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        DENSE_RANK() OVER w_all_by_gpa_ties AS gpa_dense_rank,
        ROUND(gpa_distance / NULLIF(SUM(gpa_distance) OVER (PARTITION BY major), 0), 4) AS gpa_distance_share_of_major,
        NTILE(5) OVER (ORDER BY gpa, id) AS gpa_quintile,
        MIN(gpa) OVER (PARTITION BY major) AS major_min_gpa,
        gpa_distance - LAG(gpa_distance) OVER (ORDER BY gpa, id) AS gpa_distance_change
    FROM prepared_students
    WINDOW w_all_by_gpa_ties AS (ORDER BY gpa)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        gpa_dense_rank,
        major_min_gpa,
        LOWER(REGEXP_REPLACE(TRIM(cohort), ' ', '_')) AS cohort_slug,
        CONCAT_WS('|', major, cohort) AS major_cohort_label
    FROM with_windows
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        major_min_gpa,
        cohort_slug,
        major_cohort_label
    FROM derived
    WHERE gpa_dense_rank <= 5
),
stats AS (
    -- Group-level totals
    SELECT
        YEAR(enrolled_on) AS enrolled_on_year,
        major,
        MAX(major_min_gpa) AS max_major_min_gpa,
        PERCENTILE_APPROX(gpa, 0.5) AS median_gpa,
        ROUND(100 * COUNT(CASE WHEN LOWER(student_name) LIKE '% c%' THEN 1 END) / COUNT(*), 1) AS pct_lower_student_name_like_c,
        ROUND(SUM(major_min_gpa), 2) AS total_major_min_gpa
    FROM first_rows
    GROUP BY YEAR(enrolled_on), major
    HAVING SUM(gpa) > 2.0
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE max_major_min_gpa > (SELECT AVG(max_major_min_gpa) FROM stats)
)
-- Final projection
SELECT
    enrolled_on_year,
    major,
    max_major_min_gpa,
    median_gpa,
    pct_lower_student_name_like_c,
    total_major_min_gpa,
    LEAST(median_gpa, max_major_min_gpa) AS least_median_gpa_max_major_min_gpa
FROM screened
WHERE pct_lower_student_name_like_c BETWEEN 5.0 AND 75.0
ORDER BY pct_lower_student_name_like_c
