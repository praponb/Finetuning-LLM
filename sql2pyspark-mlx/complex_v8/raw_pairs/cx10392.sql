WITH scoped_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        enrolled_on,
        is_international,
        COALESCE(gpa, 0.0) AS gpa_filled
    FROM students
),
summary AS (
    -- Group-level totals
    SELECT
        is_international,
        DATE_FORMAT(enrolled_on, 'yyyy-MM') AS enrolled_on_ym,
        PERCENTILE_APPROX(gpa_filled, 0.5) AS median_gpa_filled
    FROM scoped_students
    GROUP BY is_international, DATE_FORMAT(enrolled_on, 'yyyy-MM')
    HAVING AVG(gpa_filled) > 1.9
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE median_gpa_filled > (SELECT AVG(median_gpa_filled) FROM summary)
),
labeled AS (
    -- Add calculated columns
    SELECT
        is_international,
        enrolled_on_ym,
        CASE WHEN is_international THEN 1 ELSE 0 END AS is_international_flag,
        ABS(median_gpa_filled - 2.9) AS median_gpa_filled_distance
    FROM qualified
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        is_international,
        enrolled_on_ym,
        is_international_flag,
        LEAD(is_international_flag, 1, 0) OVER (ORDER BY median_gpa_filled_distance DESC NULLS FIRST, is_international, enrolled_on_ym) AS next_is_international_flag,
        ROUND(is_international_flag / NULLIF(SUM(is_international_flag) OVER (PARTITION BY is_international), 0), 4) AS is_international_flag_share_of_is_international,
        LAG(is_international_flag) OVER (ORDER BY median_gpa_filled_distance DESC NULLS FIRST, is_international, enrolled_on_ym) AS prev_is_international_flag,
        SUM(is_international_flag) OVER (PARTITION BY is_international) AS is_international_sum_is_international_flag,
        RANK() OVER w_all_by_median_gpa_filled_dis_2 AS median_gpa_filled_distance_rank,
        RANK() OVER w_all_by_median_gpa_filled_dis AS median_gpa_filled_distance_rank_2
    FROM labeled
    WINDOW w_all_by_median_gpa_filled_dis_2 AS (ORDER BY median_gpa_filled_distance DESC NULLS FIRST), w_all_by_median_gpa_filled_dis AS (ORDER BY median_gpa_filled_distance DESC NULLS FIRST, is_international, enrolled_on_ym)
)
SELECT
    is_international,
    next_is_international_flag,
    prev_is_international_flag,
    is_international_sum_is_international_flag,
    median_gpa_filled_distance_rank,
    median_gpa_filled_distance_rank_2,
    SUBSTRING(TRIM(enrolled_on_ym), 1, 4) AS enrolled_on_ym_prefix,
    ABS(is_international_flag - 1) AS is_international_flag_distance
FROM metrics
ORDER BY prev_is_international_flag NULLS LAST, is_international_sum_is_international_flag
