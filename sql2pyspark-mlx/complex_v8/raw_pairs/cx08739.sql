WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ABS(kwh - 51.5) AS kwh_distance,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
    WHERE site_id <= 6
      AND kwh >= 24.5
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.site_name,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM base_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(kwh_distance) OVER (PARTITION BY quality, site_zone ORDER BY kwh_distance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_distance_max_3,
        LAG(kwh, 3) OVER (PARTITION BY quality, site_zone ORDER BY kwh_distance, id) AS prev3_kwh,
        LEAD(voltage_bucket, 2, 0) OVER (PARTITION BY site_tariff ORDER BY kwh ASC, id) AS next2_voltage_bucket,
        ROUND(CUME_DIST() OVER (PARTITION BY site_zone ORDER BY kwh DESC), 3) AS kwh_cume_dist,
        ROUND(AVG(voltage_bucket) OVER (PARTITION BY site_tariff, site_zone), 2) AS site_tariff_site_zone_avg_voltage_bucket,
        ROUND(PERCENT_RANK() OVER w_site_zone_by_kwh_ties, 3) AS kwh_percent_rank,
        LAST_VALUE(kwh_distance) OVER (PARTITION BY site_zone ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh_distance,
        ROUND(voltage_bucket - AVG(voltage_bucket) OVER (PARTITION BY site_tariff, site_zone), 2) AS voltage_bucket_vs_site_tariff_site_zone_avg,
        NTH_VALUE(kwh_distance, 2) OVER (PARTITION BY quality, site_zone ORDER BY kwh_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_distance,
        SUM(kwh_distance) OVER (PARTITION BY site_tariff ORDER BY kwh ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_distance_sum_4,
        MAX(voltage) OVER (PARTITION BY site_zone ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        COUNT(*) OVER (PARTITION BY site_tariff, site_zone) AS rows_in_site_tariff_site_zone,
        MAX(voltage_bucket) OVER (PARTITION BY site_tariff ORDER BY kwh ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS voltage_bucket_max_3_prior
    FROM enriched_site
    WINDOW w_site_zone_by_kwh_ties AS (PARTITION BY site_zone ORDER BY kwh DESC)
)
-- Final projection
SELECT
    id,
    voltage,
    quality,
    kwh_distance,
    voltage_bucket,
    site_tariff,
    kwh_distance_max_3,
    prev3_kwh,
    next2_voltage_bucket,
    kwh_cume_dist,
    site_tariff_site_zone_avg_voltage_bucket,
    kwh_percent_rank,
    last_kwh_distance,
    voltage_bucket_vs_site_tariff_site_zone_avg,
    kwh_distance_sum_4,
    voltage_bucket_max_3_prior,
    CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
FROM metrics
ORDER BY voltage_bucket_max_3_prior
