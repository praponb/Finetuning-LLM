WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 235.8 THEN 'large' WHEN voltage >= 222.1 THEN 'medium' ELSE 'small' END AS voltage_tier
    FROM meter_readings
),
deduped AS (
    -- Keep the latest row per voltage_tier, quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY voltage_tier, quality ORDER BY kwh DESC, id DESC) AS recency_rank
        FROM cleaned
    ) ranked
    WHERE recency_rank = 1
),
joined_site AS (
    -- Join sites
    SELECT
        s.*,
        si.site_name,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM deduped s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IS NOT NULL
),
site_zone_stats AS (
    -- Max voltage per site_zone
    SELECT site_zone, MAX(voltage) AS max_voltage
    FROM joined_site
    GROUP BY site_zone
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_voltage,
        ROUND(s.voltage - g.max_voltage, 2) AS voltage_minus_max
    FROM joined_site s
    JOIN site_zone_stats g ON g.site_zone = s.site_zone
    WHERE s.voltage = g.max_voltage
)
-- Subtotals with ROLLUP
SELECT
    voltage_tier,
    site_id,
    GROUPING_ID() AS grouping_level,
    MAX(read_at) AS last_read_at
FROM above_average
GROUP BY ROLLUP(voltage_tier, site_id)
ORDER BY voltage_tier NULLS LAST, site_id NULLS LAST
