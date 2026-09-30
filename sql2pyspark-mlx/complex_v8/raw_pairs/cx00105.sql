WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(voltage, 0.0) AS voltage_filled,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 9
      AND kwh <= 89.8
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.voltage_filled,
        s.quality_len,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM valid s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw > 141.6
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'EST'
        )
      AND s.site_installed_kw BETWEEN 96.9 AND 386.6
),
site_zone_stats AS (
    -- Avg voltage_filled per site_zone
    SELECT site_zone, AVG(voltage_filled) AS avg_voltage_filled
    FROM filtered
    GROUP BY site_zone
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage_filled,
        ROUND(s.voltage_filled - g.avg_voltage_filled, 2) AS voltage_filled_minus_avg
    FROM filtered s
    JOIN site_zone_stats g ON g.site_zone = s.site_zone
    WHERE s.voltage_filled > g.avg_voltage_filled
)
SELECT
    site_id,
    site_zone,
    quality_len,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
    COUNT(*) AS row_count,
    MAX(voltage) AS max_voltage
FROM above_average
GROUP BY GROUPING SETS ((site_id, site_zone, quality_len), (site_id), ())
ORDER BY site_id NULLS LAST, site_zone NULLS LAST, quality_len NULLS LAST
