WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM meter_readings
    WHERE voltage >= 216.1
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.read_at_label,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM cleaned s
    JOIN meter_readings m ON m.site_id = s.site_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(reading_voltage * 0.25, 2) AS reading_voltage_adj,
        ROUND(voltage * 1.5, 2) AS voltage_adj
    FROM detail_rows_readings
),
latest_per_key AS (
    -- Keep the latest row per quality, site_id
    SELECT
        id,
        site_id,
        voltage,
        quality,
        read_at_label,
        reading_id,
        reading_voltage,
        reading_kwh,
        reading_read_at,
        reading_voltage_adj,
        voltage_adj
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY reading_kwh DESC, id DESC, reading_id DESC) AS recency_rank
        FROM enriched
    ) ranked
    WHERE recency_rank = 1
),
site_id_stats AS (
    -- Avg reading_voltage per site_id
    SELECT site_id, AVG(reading_voltage) AS avg_reading_voltage
    FROM latest_per_key
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_reading_voltage,
        ROUND(s.reading_voltage - g.avg_reading_voltage, 2) AS reading_voltage_minus_avg
    FROM latest_per_key s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.reading_voltage > g.avg_reading_voltage
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.read_at_label,
        s.reading_id,
        s.reading_voltage,
        s.reading_kwh,
        s.reading_read_at,
        s.reading_voltage_adj,
        s.voltage_adj,
        s.avg_reading_voltage,
        s.reading_voltage_minus_avg,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM vs_group s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on IS NOT NULL
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    quality,
    read_at_label,
    reading_id,
    reading_kwh,
    reading_read_at,
    reading_voltage_adj,
    voltage_adj,
    reading_voltage_minus_avg,
    site_installed_kw,
    COALESCE(reading_voltage_adj, 0.0) AS reading_voltage_adj_filled,
    CONCAT(read_at_label, '#', CAST(reading_id AS STRING)) AS read_at_label_ref
FROM with_details_site
ORDER BY site_id DESC, reading_read_at
