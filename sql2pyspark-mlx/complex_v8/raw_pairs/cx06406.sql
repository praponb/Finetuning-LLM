WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 233.2 THEN 'A' WHEN voltage >= 218.2 THEN 'B' ELSE 'C' END AS voltage_tier
    FROM meter_readings
    WHERE site_id <= 7
      AND HOUR(read_at) BETWEEN 9 AND 12
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.voltage_tier,
        si.tariff AS site_tariff,
        si.site_name
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        voltage_tier,
        site_tariff,
        site_name,
        ROUND(AVG(kwh) OVER (PARTITION BY quality ORDER BY kwh ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3,
        ROUND(CUME_DIST() OVER (ORDER BY read_at DESC), 3) AS read_at_cume_dist,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY quality, site_id)) AS distinct_site_tariff_in_quality_site_id,
        voltage - LAG(voltage) OVER (ORDER BY read_at DESC, id) AS voltage_change,
        kwh - LAG(kwh) OVER (ORDER BY read_at DESC, id) AS kwh_change,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY read_at DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, site_id), 2) AS kwh_vs_quality_site_id_avg,
        NTILE(3) OVER (PARTITION BY site_tariff ORDER BY read_at DESC NULLS FIRST, id) AS read_at_tercile,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at DESC), 3) AS read_at_percent_rank,
        ROUND(voltage - AVG(voltage) OVER w_quality_site_id, 2) AS voltage_vs_quality_site_id_avg
    FROM joined_site
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage_tier,
        site_tariff,
        site_name,
        read_at_cume_dist,
        distinct_site_tariff_in_quality_site_id,
        voltage_change,
        kwh_change,
        minutes_since_prev_read_at,
        kwh_vs_quality_site_id_avg,
        read_at_tercile,
        read_at_percent_rank,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM with_windows
    WHERE UPPER(TRIM(site_name)) LIKE 'R%'
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage_tier,
    site_tariff,
    read_at_cume_dist,
    distinct_site_tariff_in_quality_site_id,
    kwh_change,
    minutes_since_prev_read_at,
    kwh_vs_quality_site_id_avg,
    read_at_tercile,
    days_since_read_at,
    CONCAT_WS(': ', voltage_tier, site_name) AS voltage_tier_site_name_label
FROM labeled
ORDER BY distinct_site_tariff_in_quality_site_id
