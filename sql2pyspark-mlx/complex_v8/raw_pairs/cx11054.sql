WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY voltage DESC), 3) AS voltage_cume_dist,
        voltage - LAG(voltage) OVER (PARTITION BY site_id ORDER BY voltage DESC, id) AS voltage_change,
        LEAD(kwh, 3, 0) OVER w_site_id_by_voltage AS next3_kwh
    FROM valid_readings
    WINDOW w_site_id_by_voltage AS (PARTITION BY site_id ORDER BY voltage DESC, id)
),
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone <> 'Z4'
              AND si.installed_kw > 93.3
        )
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at
    FROM screened s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    voltage_cume_dist,
    next3_kwh,
    last_reading_read_at,
    last_reading_read_at + INTERVAL 30 MINUTES AS last_reading_read_at_plus_30m,
    ROUND(voltage_change * 1.5, 2) AS voltage_change_adj
FROM with_stats
WHERE voltage_cume_dist BETWEEN 0.1 AND 0.8
ORDER BY site_id DESC NULLS FIRST, kwh
