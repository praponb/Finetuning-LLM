WITH valid_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        CONCAT(major, '#', CAST(id AS STRING)) AS major_ref
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'G%'
),
per_group AS (
    -- Group-level totals
    SELECT
        is_international,
        COUNT(*) AS row_count,
        SUM(gpa) FILTER (WHERE is_international = TRUE) AS gpa_is_international_true
    FROM valid_students
    WHERE major_ref IS NOT NULL
    GROUP BY is_international
),
enriched AS (
    -- Derive helper columns
    SELECT *, ABS(row_count - 8) AS row_count_distance
    FROM per_group
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        is_international,
        row_count,
        gpa_is_international_true,
        row_count_distance,
        ROUND(AVG(row_count_distance) OVER (PARTITION BY is_international, row_count), 2) AS is_international_row_count_avg_row_count_distanc,
        LAST_VALUE(gpa_is_international_true) OVER (PARTITION BY row_count_distance, is_international ORDER BY row_count_distance, is_international ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_gpa_is_international_true,
        MAX(row_count_distance) OVER (PARTITION BY row_count_distance, is_international ORDER BY row_count_distance, is_international ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_row_count_distance_so_far
    FROM enriched
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM sequenced
    WHERE is_international_row_count_avg_row_count_distanc > (SELECT MIN(is_international_row_count_avg_row_count_distanc) FROM sequenced)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(row_count_distance - AVG(row_count_distance) OVER (PARTITION BY row_count), 2) AS row_count_distance_vs_row_count_avg,
        RANK() OVER w_is_international_by_row_coun AS row_count_distance_rank,
        MAX(row_count_distance) OVER (PARTITION BY is_international ORDER BY row_count_distance, is_international ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS row_count_distance_max_7,
        COUNT(*) OVER w_row_count AS rows_in_row_count,
        ROW_NUMBER() OVER (PARTITION BY is_international ORDER BY row_count_distance, is_international) AS rn
    FROM qualified
    WINDOW w_is_international_by_row_coun AS (PARTITION BY is_international ORDER BY row_count_distance, is_international), w_row_count AS (PARTITION BY row_count)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        is_international,
        row_count,
        gpa_is_international_true,
        row_count_distance,
        is_international_row_count_avg_row_count_distanc,
        last_gpa_is_international_true,
        max_row_count_distance_so_far,
        row_count_distance_vs_row_count_avg,
        row_count_distance_max_7,
        rows_in_row_count,
        rn,
        ROUND(CUME_DIST() OVER (PARTITION BY rn ORDER BY row_count_distance_max_7 DESC, is_international), 3) AS row_count_distance_max_7_cume_dist,
        LAST_VALUE(gpa_is_international_true) OVER (PARTITION BY is_international ORDER BY is_international_row_count_avg_row_count_distanc NULLS LAST, is_international ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_gpa_is_international_true_2,
        SUM(row_count_distance_max_7) OVER (PARTITION BY row_count_distance_rank, row_count_distance) AS row_count_distance_rank_row_count_distance_sum_r,
        ROUND(PERCENT_RANK() OVER w_rn_by_row_count_distance_max_2, 3) AS row_count_distance_max_7_percent_rank,
        NTH_VALUE(gpa_is_international_true, 2) OVER (PARTITION BY is_international ORDER BY is_international_row_count_avg_row_count_distanc NULLS LAST, is_international ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa_is_international_true
    FROM metrics
    WINDOW w_rn_by_row_count_distance_max_2 AS (PARTITION BY rn ORDER BY row_count_distance_max_7 DESC)
)
SELECT
    rn,
    row_count,
    row_count_distance,
    ROUND(AVG(is_international_row_count_avg_row_count_distanc), 2) AS avg_is_international_row_count_avg_row_count_dis,
    ROUND(AVG(rows_in_row_count), 2) AS avg_rows_in_row_count,
    SUM(row_count) AS total_row_count
FROM windowed
GROUP BY CUBE(rn, row_count, row_count_distance)
ORDER BY rn NULLS LAST, row_count NULLS LAST, row_count_distance NULLS LAST
