WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE kwh >= 43.1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        SUM(voltage) OVER (ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS voltage_sum_7,
        NTILE(4) OVER (ORDER BY kwh, id) AS kwh_quartile,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality, site_id), 0), 4) AS voltage_share_of_quality_site_id,
        MIN(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_kwh
    FROM recent_readings
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(kwh_quartile) IGNORE NULLS OVER (ORDER BY voltage_sum_7, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh_quartile,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        RANK() OVER (ORDER BY voltage_sum_7) AS voltage_sum_7_rank,
        COUNT(*) OVER (ORDER BY voltage_sum_7, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(quality_site_id_min_kwh) OVER (ORDER BY voltage_sum_7, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quality_site_id_min_kwh
    FROM with_windows
    WINDOW w_site_id AS (PARTITION BY site_id)
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.voltage_sum_7,
        s.kwh_quartile,
        s.voltage_share_of_quality_site_id,
        s.quality_site_id_min_kwh,
        s.distinct_quality_in_site_id,
        s.voltage_sum_7_rank,
        s.running_quality_site_id_min_kwh,
        si.commissioned_on AS site_commissioned_on
    FROM with_windows_2 s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    quality,
    kwh_quartile,
    voltage_share_of_quality_site_id,
    voltage_sum_7_rank,
    running_quality_site_id_min_kwh,
    CONCAT(quality, '#', CAST(kwh_quartile AS STRING)) AS quality_ref
FROM enriched
WHERE quality = 'OK'
ORDER BY kwh_quartile
