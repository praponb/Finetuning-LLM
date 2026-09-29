WITH recent AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        ABS(gpa - 3.3) AS gpa_distance
    FROM students
    WHERE LOWER(student_name) LIKE '%an%'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        cohort,
        enrolled_on,
        is_international,
        gpa_distance,
        MAX(gpa_distance) OVER (PARTITION BY YEAR(enrolled_on) ORDER BY gpa_distance DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS gpa_distance_max_3,
        ROUND(AVG(gpa) OVER (PARTITION BY YEAR(enrolled_on) ORDER BY gpa_distance DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS gpa_avg_3,
        COUNT(*) OVER (PARTITION BY major, cohort) AS rows_in_major_cohort,
        ROUND(gpa_distance - AVG(gpa_distance) OVER (PARTITION BY major, cohort), 2) AS gpa_distance_vs_major_cohort_avg,
        LAG(gpa) OVER (PARTITION BY YEAR(enrolled_on), is_international ORDER BY gpa_distance DESC, id) AS prev_gpa,
        LAG(gpa, 2) OVER (PARTITION BY YEAR(enrolled_on), is_international ORDER BY gpa_distance DESC, id) AS prev2_gpa,
        ROUND(AVG(gpa_distance) OVER (PARTITION BY YEAR(enrolled_on), is_international ORDER BY gpa_distance DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS gpa_distance_avg_3,
        LAST_VALUE(enrolled_on) IGNORE NULLS OVER (PARTITION BY cohort, major ORDER BY gpa ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_on,
        SUM(gpa_distance) OVER (PARTITION BY YEAR(enrolled_on) ORDER BY gpa_distance DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_gpa_distance,
        NTH_VALUE(gpa, 2) OVER (PARTITION BY cohort, major ORDER BY gpa ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa,
        gpa - LAG(gpa) OVER (PARTITION BY cohort, major ORDER BY gpa ASC NULLS LAST, id) AS gpa_change
    FROM recent
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        is_international,
        DATE_FORMAT(last_known_enrolled_on, 'yyyy-MM') AS last_known_enrolled_on_ym,
        YEAR(enrolled_on) AS enrolled_on_year,
        MAX(rows_in_major_cohort) AS max_rows_in_major_cohort,
        COUNT(*) AS cnt,
        COUNT(DISTINCT cohort) AS distinct_cohorts,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows
    GROUP BY 1, 2, 3
)
SELECT
    is_international,
    last_known_enrolled_on_ym,
    max_rows_in_major_cohort,
    cnt,
    distinct_cohorts,
    distinct_ids,
    CASE WHEN distinct_ids >= 6 THEN 'large' WHEN distinct_ids >= 5 THEN 'medium' ELSE 'small' END AS distinct_ids_tier
FROM per_group
WHERE max_rows_in_major_cohort > 5
