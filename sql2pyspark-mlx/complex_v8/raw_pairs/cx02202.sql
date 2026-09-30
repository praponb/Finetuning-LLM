WITH cleaned AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        is_international,
        ABS(gpa - 2.7) AS gpa_distance
    FROM students
    WHERE gpa < 3.2
      AND major = 'Biz'
),
latest_per_key AS (
    -- Keep the latest row per cohort
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        gpa,
        is_international,
        gpa_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY gpa_distance DESC, id DESC) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY is_international ORDER BY gpa DESC), 3) AS gpa_cume_dist,
        ROUND(gpa_distance / NULLIF(SUM(gpa_distance) OVER (PARTITION BY cohort), 0), 4) AS gpa_distance_share_of_cohort,
        gpa - LAG(gpa) OVER w_is_international_by_gpa AS gpa_change
    FROM latest_per_key
    WINDOW w_is_international_by_gpa AS (PARTITION BY is_international ORDER BY gpa DESC, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', enrolled_on) AS enrolled_on_month,
        ROUND(STDDEV(gpa_cume_dist), 2) AS stddev_gpa_cume_dist,
        ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list,
        MIN(gpa_change) AS min_gpa_change,
        SUM(gpa_change) AS total_gpa_change,
        SUM(gpa_distance_share_of_cohort) AS total_gpa_distance_share_of_cohort,
        SUM(gpa_cume_dist) AS total_gpa_cume_dist
    FROM with_trends
    GROUP BY DATE_TRUNC('MONTH', enrolled_on)
    HAVING AVG(gpa_distance_share_of_cohort) > 0.3
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(total_gpa_cume_dist) IGNORE NULLS OVER (ORDER BY stddev_gpa_cume_dist ASC, enrolled_on_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_total_gpa_cume_dist,
        NTILE(10) OVER (ORDER BY stddev_gpa_cume_dist ASC, enrolled_on_month) AS stddev_gpa_cume_dist_decile
    FROM per_group
)
-- Final projection
SELECT
    enrolled_on_month,
    stddev_gpa_cume_dist,
    student_name_list,
    min_gpa_change,
    total_gpa_change,
    total_gpa_distance_share_of_cohort,
    total_gpa_cume_dist,
    first_known_total_gpa_cume_dist,
    stddev_gpa_cume_dist_decile
FROM with_trends_2
ORDER BY min_gpa_change DESC
