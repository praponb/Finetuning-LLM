WITH recent AS (
    -- Filter students
    SELECT
        id,
        cohort,
        major,
        gpa,
        CONCAT(cohort, '#', CAST(id AS STRING)) AS cohort_ref,
        DATE_FORMAT(enrolled_on, 'yyyy-MM') AS enrolled_on_label
    FROM students
    WHERE gpa BETWEEN 2.1 AND 3.4
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        cohort,
        major,
        cohort_ref,
        enrolled_on_label,
        SUM(gpa) OVER (PARTITION BY major, cohort ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_gpa,
        MAX(gpa) OVER (PARTITION BY cohort ORDER BY gpa DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS gpa_max_4,
        SIZE(COLLECT_SET(cohort) OVER w_major) AS distinct_cohort_in_major,
        NTH_VALUE(gpa, 2) OVER (PARTITION BY major ORDER BY gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa,
        LEAD(gpa, 3) OVER w_cohort_by_gpa AS next3_gpa,
        SUM(gpa) OVER (ORDER BY gpa DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS gpa_sum_3_prior,
        ROUND(gpa - AVG(gpa) OVER (PARTITION BY major), 2) AS gpa_vs_major_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY cohort, major ORDER BY gpa DESC, id), 3) AS gpa_cume_dist,
        LAG(gpa, 2) OVER (PARTITION BY major, cohort ORDER BY gpa, id) AS prev2_gpa,
        NTILE(5) OVER (PARTITION BY major, cohort ORDER BY gpa, id) AS gpa_quintile,
        ROW_NUMBER() OVER (PARTITION BY cohort, major ORDER BY gpa DESC, id) AS gpa_seq,
        RANK() OVER (PARTITION BY cohort, major ORDER BY gpa DESC, id) AS gpa_rank,
        SUM(gpa) OVER (PARTITION BY major) AS major_sum_gpa,
        COUNT(*) OVER w_major AS rows_in_major,
        ROUND(PERCENT_RANK() OVER (PARTITION BY cohort, major ORDER BY gpa DESC), 3) AS gpa_percent_rank
    FROM recent
    WINDOW w_major AS (PARTITION BY major), w_cohort_by_gpa AS (PARTITION BY cohort ORDER BY gpa DESC, id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        cohort,
        cohort_ref,
        enrolled_on_label,
        running_gpa,
        gpa_max_4,
        distinct_cohort_in_major,
        second_gpa,
        next3_gpa,
        gpa_sum_3_prior,
        gpa_vs_major_avg,
        gpa_cume_dist,
        prev2_gpa,
        gpa_quintile,
        gpa_rank,
        major_sum_gpa,
        CONCAT(cohort_ref, '#', CAST(distinct_cohort_in_major AS STRING)) AS cohort_ref_ref,
        LENGTH(TRIM(cohort_ref)) AS cohort_ref_len,
        COALESCE(gpa_vs_major_avg, 0.0) AS gpa_vs_major_avg_filled
    FROM metrics
    WHERE running_gpa >= 11.1
),
totals AS (
    -- Summarise per group
    SELECT
        distinct_cohort_in_major,
        CASE WHEN gpa_vs_major_avg_filled >= 1.2 THEN 'A' WHEN gpa_vs_major_avg_filled >= -2.4 THEN 'B' ELSE 'C' END AS gpa_vs_major_avg_filled_tier,
        gpa_rank,
        ARRAY_SORT(COLLECT_SET(cohort)) AS cohort_list,
        SUM(gpa_quintile) AS total_gpa_quintile,
        COUNT(*) FILTER (WHERE cohort = 'C2024') AS n_cohort_c2024
    FROM derived
    WHERE cohort IN ('C2022', 'C2023', 'C2024')
    GROUP BY 1, 2, 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (ORDER BY total_gpa_quintile NULLS LAST, distinct_cohort_in_major, gpa_vs_major_avg_filled_tier, gpa_rank) AS total_gpa_quintile_rank,
        ROUND(gpa_rank / NULLIF(SUM(gpa_rank) OVER (PARTITION BY gpa_rank, gpa_vs_major_avg_filled_tier), 0), 4) AS gpa_rank_share_of_gpa_rank_gpa_vs_major_avg_fill,
        distinct_cohort_in_major - LAG(distinct_cohort_in_major) OVER (ORDER BY total_gpa_quintile NULLS LAST, distinct_cohort_in_major, gpa_vs_major_avg_filled_tier, gpa_rank) AS distinct_cohort_in_major_change
    FROM totals
)
SELECT
    distinct_cohort_in_major,
    gpa_vs_major_avg_filled_tier,
    gpa_rank,
    n_cohort_c2024,
    total_gpa_quintile_rank,
    gpa_rank_share_of_gpa_rank_gpa_vs_major_avg_fill,
    distinct_cohort_in_major_change,
    SIZE(cohort_list) AS cohort_list_count
FROM ranked
ORDER BY gpa_vs_major_avg_filled_tier
