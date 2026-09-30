WITH prepared AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        LENGTH(TRIM(site_name)) AS site_name_len,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM sites
    WHERE tariff <> 'EV'
      AND UPPER(TRIM(site_name)) LIKE 'S%'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(voltage) AS max_reading_voltage, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.max_reading_voltage,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM prepared s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        site_name,
        installed_kw,
        site_name_len,
        installed_kw_bucket,
        max_reading_voltage,
        reading_count,
        ROUND(installed_kw * 0.25, 2) AS installed_kw_adj,
        ROUND(max_reading_voltage * 0.25, 2) AS max_reading_voltage_adj,
        CONCAT_WS('|', tariff, site_name) AS tariff_site_name_label
    FROM profiled
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.kwh) FROM meter_readings m WHERE m.site_id = s.id) AS max_reading_kwh
    FROM enriched s
)
-- Subtotals with ROLLUP
SELECT
    reading_count,
    site_name_len,
    ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list
FROM with_stats
GROUP BY ROLLUP(reading_count, site_name_len)
ORDER BY reading_count NULLS LAST, site_name_len NULLS LAST
