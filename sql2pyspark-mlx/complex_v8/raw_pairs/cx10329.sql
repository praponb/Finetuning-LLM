WITH filtered_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        ABS(gpa - 2.7) AS gpa_distance,
        LENGTH(TRIM(cohort)) AS cohort_len,
        CONCAT(cohort, '#', CAST(id AS STRING)) AS cohort_ref
    FROM students
    WHERE gpa < 3.2
      AND UPPER(TRIM(student_name)) LIKE 'B%'
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered_students
    WHERE cohort_len > (SELECT MIN(cohort_len) FROM filtered_students)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        gpa_distance,
        cohort_len,
        ROUND(AVG(gpa_distance) OVER (PARTITION BY cohort ORDER BY gpa_distance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS gpa_distance_avg_3,
        ROUND(gpa_distance - AVG(gpa_distance) OVER (PARTITION BY is_international, cohort_len), 2) AS gpa_distance_vs_is_international_cohort_len_avg,
        LEAD(gpa_distance, 2) OVER (PARTITION BY cohort_len ORDER BY gpa_distance DESC, id) AS next2_gpa_distance
    FROM with_activity
)
-- Subtotals with CUBE
SELECT
    COALESCE(cohort, 'ALL') AS cohort,
    is_international,
    ROUND(SUM(gpa_distance_avg_3), 2) AS total_gpa_distance_avg_3,
    ROUND(SUM(gpa_distance_avg_3) / NULLIF(SUM(cohort_len), 0), 3) AS gpa_distance_avg_3_to_cohort_len_ratio,
    COUNT(DISTINCT major) AS distinct_majors
FROM with_windows
GROUP BY CUBE(cohort, is_international)
ORDER BY cohort NULLS LAST, is_international NULLS LAST
