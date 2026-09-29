WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE site_id <= 5
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        si.site_name,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM cleaned_readings s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw > 96.9
    WHERE si.zone = 'Z4'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_zone, site_tariff)) AS distinct_site_id_in_site_zone_site_tariff,
        MAX(kwh) OVER w_site_tariff_site_zone AS site_tariff_site_zone_max_kwh,
        DENSE_RANK() OVER (PARTITION BY site_tariff ORDER BY voltage DESC, id) AS voltage_dense_rank,
        DENSE_RANK() OVER (ORDER BY voltage DESC NULLS FIRST, id) AS voltage_dense_rank_2,
        COUNT(*) OVER (PARTITION BY site_zone, site_tariff) AS rows_in_site_zone_site_tariff,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_tariff, site_zone)) AS distinct_site_id_in_site_tariff_site_zone,
        MAX(voltage) OVER (ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_zone, site_tariff), 0), 4) AS voltage_share_of_site_zone_site_tariff,
        voltage - LAG(voltage) OVER w_site_id_site_zone_by_voltage AS voltage_change,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, site_zone ORDER BY voltage, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_2,
        NTILE(5) OVER w_site_zone_site_id_by_voltage AS voltage_quintile,
        SUM(kwh) OVER (PARTITION BY site_zone, site_id ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_sum_6_prior
    FROM enriched
    WINDOW w_site_tariff_site_zone AS (PARTITION BY site_tariff, site_zone), w_site_id_site_zone_by_voltage AS (PARTITION BY site_id, site_zone ORDER BY voltage, id), w_site_id AS (PARTITION BY site_id), w_site_zone_site_id_by_voltage AS (PARTITION BY site_zone, site_id ORDER BY voltage, id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        voltage,
        site_zone,
        distinct_site_id_in_site_zone_site_tariff,
        voltage_dense_rank,
        rows_in_site_zone_site_tariff,
        distinct_site_id_in_site_tariff_site_zone,
        voltage_share_of_site_zone_site_tariff,
        voltage_change,
        rows_in_site_id,
        kwh_avg_2,
        voltage_quintile,
        CONCAT_WS(' / ', site_zone, site_name) AS site_zone_site_name_label,
        LOWER(TRIM(site_name)) AS site_name_clean,
        FLOOR(max_voltage_so_far / 500) * 500 AS max_voltage_so_far_bucket
    FROM ranked
    WHERE voltage_quintile >= 3
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(max_voltage_so_far_bucket) OVER (PARTITION BY distinct_site_id_in_site_tariff_site_zone), 2) AS distinct_site_id_in_site_tariff_site_zone_avg_ma,
        RANK() OVER (ORDER BY kwh_avg_2) AS kwh_avg_2_rank,
        SIZE(COLLECT_SET(site_zone) OVER (PARTITION BY distinct_site_id_in_site_tariff_site_zone)) AS distinct_site_zone_in_distinct_site_id_in_site_t,
        ROUND(AVG(rows_in_site_id) OVER (ORDER BY kwh_avg_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS rows_in_site_id_avg_4,
        MAX(max_voltage_so_far_bucket) OVER (ORDER BY kwh_avg_2, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_voltage_so_far_bucket_max_2,
        DENSE_RANK() OVER (ORDER BY kwh_avg_2) AS kwh_avg_2_dense_rank,
        COUNT(*) OVER (PARTITION BY distinct_site_id_in_site_tariff_site_zone) AS rows_in_distinct_site_id_in_site_tariff_site_zon
    FROM derived
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    site_zone,
    voltage_dense_rank,
    rows_in_site_zone_site_tariff,
    distinct_site_id_in_site_tariff_site_zone,
    voltage_share_of_site_zone_site_tariff,
    voltage_change,
    rows_in_site_id,
    kwh_avg_2,
    site_zone_site_name_label,
    site_name_clean,
    max_voltage_so_far_bucket,
    distinct_site_id_in_site_tariff_site_zone_avg_ma,
    kwh_avg_2_rank,
    max_voltage_so_far_bucket_max_2,
    kwh_avg_2_dense_rank,
    rows_in_distinct_site_id_in_site_tariff_site_zon,
    SUBSTRING(TRIM(site_zone), 1, 2) AS site_zone_prefix,
    voltage_dense_rank + kwh_avg_2_rank AS voltage_dense_rank_plus_kwh_avg_2_rank
FROM windowed
WHERE distinct_site_id_in_site_tariff_site_zone >= 3
ORDER BY kwh_avg_2, site_zone_site_name_label DESC, id
LIMIT 5
