WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled,
        COALESCE(voltage, 0.0) AS voltage_filled,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE (read_at >= TIMESTAMP '2025-02-06 15:00:00' OR voltage > 220.2)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        quality,
        kwh_filled,
        voltage_filled,
        LAST_VALUE(kwh_bucket) IGNORE NULLS OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh_bucket,
        ROUND(CUME_DIST() OVER w_all_by_kwh, 3) AS kwh_cume_dist,
        ROUND(CUME_DIST() OVER w_all_by_kwh_ties, 3) AS kwh_cume_dist_2,
        ROUND(AVG(kwh_bucket) OVER (PARTITION BY quality, site_id), 2) AS quality_site_id_avg_kwh_bucket,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id,
        FIRST_VALUE(kwh_bucket) OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh_bucket
    FROM recent
    WINDOW w_all_by_kwh AS (ORDER BY kwh, id), w_all_by_kwh_ties AS (ORDER BY kwh)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        quality,
        kwh_filled,
        voltage_filled,
        last_known_kwh_bucket,
        kwh_cume_dist,
        quality_site_id_avg_kwh_bucket,
        kwh_cume_dist_2 - last_known_kwh_bucket AS kwh_cume_dist_2_minus_last_known_kwh_bucket,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM ranked
),
scored AS (
    -- Derive helper columns
    SELECT *, INITCAP(TRIM(quality)) AS quality_clean
    FROM annotated
),
annotated_2 AS (
    -- Add calculated columns
    SELECT
        id,
        quality,
        voltage_filled,
        last_known_kwh_bucket,
        kwh_cume_dist,
        quality_site_id_avg_kwh_bucket,
        kwh_cume_dist_2_minus_last_known_kwh_bucket,
        quality_prefix,
        quality_clean,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix_2
    FROM scored
),
unique_rows AS (
    -- Keep the earliest row per quality
    SELECT
        id,
        quality,
        voltage_filled,
        last_known_kwh_bucket,
        kwh_cume_dist,
        quality_site_id_avg_kwh_bucket,
        kwh_cume_dist_2_minus_last_known_kwh_bucket,
        quality_prefix,
        quality_clean,
        quality_prefix_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY quality_site_id_avg_kwh_bucket, id) AS row_num
        FROM annotated_2
    ) ranked
    WHERE row_num = 1
),
deduped AS (
    -- Keep the earliest row per quality
    SELECT
        id,
        quality,
        voltage_filled,
        last_known_kwh_bucket,
        kwh_cume_dist,
        quality_site_id_avg_kwh_bucket,
        kwh_cume_dist_2_minus_last_known_kwh_bucket,
        quality_prefix,
        quality_clean,
        quality_prefix_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY quality_site_id_avg_kwh_bucket, id) AS rn
        FROM unique_rows
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    voltage_filled,
    kwh_cume_dist,
    quality_site_id_avg_kwh_bucket,
    kwh_cume_dist_2_minus_last_known_kwh_bucket,
    quality_prefix,
    quality_prefix_2
FROM deduped
ORDER BY quality_site_id_avg_kwh_bucket DESC NULLS FIRST, kwh_cume_dist_2_minus_last_known_kwh_bucket
