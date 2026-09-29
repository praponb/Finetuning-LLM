WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 85.7
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM base
),
grouped AS (
    -- Summarise per group
    SELECT
        quality,
        SUM(CASE WHEN quality NOT IN ('BAD', 'EST') THEN 1 ELSE 0 END) AS n_quality_not_in_bad_est,
        COUNT(*) FILTER (WHERE quality IS NOT NULL) AS n_quality_is_not_null,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM enriched
    GROUP BY quality
    HAVING SUM(voltage) > 218.2
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(n_quality_is_not_null / 2) * 2 AS n_quality_is_not_null_bucket,
        COALESCE(n_quality_not_in_bad_est, 0) AS n_quality_not_in_bad_est_filled,
        ABS(n_quality_is_not_null - 6) AS n_quality_is_not_null_distance
    FROM grouped
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM enriched_2
    WHERE n_quality_not_in_bad_est_filled > (SELECT MIN(n_quality_not_in_bad_est_filled) FROM enriched_2)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        quality,
        n_quality_not_in_bad_est,
        n_quality_is_not_null,
        quality_list,
        n_quality_is_not_null_bucket,
        n_quality_not_in_bad_est_filled,
        RANK() OVER (PARTITION BY n_quality_is_not_null_distance, n_quality_not_in_bad_est ORDER BY n_quality_not_in_bad_est_filled DESC, quality) AS n_quality_not_in_bad_est_filled_rank,
        ROUND(n_quality_not_in_bad_est / NULLIF(SUM(n_quality_not_in_bad_est) OVER (PARTITION BY n_quality_is_not_null), 0), 4) AS n_quality_not_in_bad_est_share_of_n_quality_is_n,
        LAG(n_quality_is_not_null, 3, 0) OVER (PARTITION BY n_quality_is_not_null_distance, n_quality_not_in_bad_est ORDER BY n_quality_not_in_bad_est_filled DESC, quality) AS prev3_n_quality_is_not_null,
        SUM(n_quality_is_not_null_bucket) OVER (PARTITION BY n_quality_is_not_null_distance, n_quality_not_in_bad_est ORDER BY n_quality_not_in_bad_est_filled DESC, quality ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS n_quality_is_not_null_bucket_sum_6_prior
    FROM screened
),
top_per_group AS (
    -- First row per group
    SELECT
        quality,
        n_quality_not_in_bad_est,
        n_quality_is_not_null,
        quality_list,
        n_quality_is_not_null_bucket,
        n_quality_not_in_bad_est_filled,
        n_quality_not_in_bad_est_share_of_n_quality_is_n,
        prev3_n_quality_is_not_null,
        n_quality_is_not_null_bucket_sum_6_prior
    FROM metrics
    WHERE n_quality_not_in_bad_est_filled_rank = 1
)
SELECT
    prev3_n_quality_is_not_null,
    n_quality_not_in_bad_est_filled,
    quality,
    GROUPING(prev3_n_quality_is_not_null) AS is_total_prev3_n_quality_is_not_null,
    SUM(CASE WHEN quality <> 'EST' THEN n_quality_not_in_bad_est ELSE 0 END) AS n_quality_not_in_bad_est_quality_est,
    ROUND(SUM(n_quality_is_not_null_bucket) / NULLIF(SUM(n_quality_is_not_null_bucket_sum_6_prior), 0), 3) AS n_quality_is_not_null_bucket_to_n_quality_is_not
FROM top_per_group
GROUP BY ROLLUP(prev3_n_quality_is_not_null, n_quality_not_in_bad_est_filled, quality)
