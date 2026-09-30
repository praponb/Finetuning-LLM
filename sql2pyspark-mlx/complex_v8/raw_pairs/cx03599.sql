WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        LOWER(TRIM(quality)) AS quality_clean
    FROM meter_readings
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.voltage_bucket,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM recent_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        kwh,
        quality,
        voltage_bucket,
        site_commissioned_on,
        site_tariff,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_tariff, quality)) AS distinct_site_id_in_site_tariff_quality,
        ROUND(AVG(voltage_bucket) OVER (PARTITION BY site_id ORDER BY voltage_bucket DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_bucket_avg_1_prior,
        ROUND(AVG(voltage_bucket) OVER (PARTITION BY site_tariff, quality), 2) AS site_tariff_quality_avg_voltage_bucket,
        LEAD(site_commissioned_on, 3) OVER w_site_id_site_tariff_by_site AS next3_site_commissioned_on,
        COUNT(*) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM combined_details
    WINDOW w_site_id_site_tariff_by_site AS (PARTITION BY site_id, site_tariff ORDER BY site_commissioned_on ASC, id)
),
derived AS (
    -- Label and score rows
    SELECT *, COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM metrics
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        MAX(kwh) OVER (PARTITION BY site_id) AS site_id_max_kwh,
        SIZE(COLLECT_SET(quality_or_unknown) OVER w_site_id) AS distinct_quality_or_unknown_in_site_id,
        ROUND(AVG(running_count) OVER (PARTITION BY site_tariff), 2) AS site_tariff_avg_running_count,
        LEAD(voltage_bucket_avg_1_prior, 1, 0) OVER (PARTITION BY YEAR(site_commissioned_on) ORDER BY kwh, id) AS next_voltage_bucket_avg_1_prior,
        NTILE(3) OVER w_quality_or_unknown_by_kwh AS kwh_tercile,
        LAST_VALUE(voltage_bucket_avg_1_prior) IGNORE NULLS OVER (PARTITION BY YEAR(site_commissioned_on) ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage_bucket_avg_1_prior
    FROM derived
    WINDOW w_site_id AS (PARTITION BY site_id), w_quality_or_unknown_by_kwh AS (PARTITION BY quality_or_unknown ORDER BY kwh DESC, id)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    quality,
    voltage_bucket,
    site_commissioned_on,
    site_tariff,
    distinct_site_id_in_site_tariff_quality,
    site_tariff_quality_avg_voltage_bucket,
    running_count,
    rows_in_site_id,
    ROUND(kwh / NULLIF(rows_in_site_id, 0), 3) AS kwh_per_rows_in_site_id,
    LOWER(TRIM(quality)) AS quality_clean
FROM metrics_2
ORDER BY voltage_bucket DESC, running_count
