WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND kwh BETWEEN 15.9 AND 109.0
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.days_since_read_at,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
),
site_id_stats AS (
    -- Avg voltage per site_id
    SELECT site_id, AVG(voltage) AS avg_voltage
    FROM combined_details_site
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM combined_details_site s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage > g.avg_voltage
),
latest_per_key AS (
    -- Keep the latest row per site_zone, site_id
    SELECT
        id,
        site_id,
        voltage,
        days_since_read_at,
        site_commissioned_on,
        site_zone,
        avg_voltage,
        voltage_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_zone, site_id ORDER BY voltage DESC, id DESC) AS row_num
        FROM outliers
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    site_zone,
    site_id,
    ROUND(AVG(days_since_read_at), 2) AS avg_days_since_read_at,
    COUNT(*) AS row_count,
    COUNT(DISTINCT site_id) AS distinct_site_ids
FROM latest_per_key
GROUP BY CUBE(site_zone, site_id)
ORDER BY site_zone NULLS LAST, site_id NULLS LAST
