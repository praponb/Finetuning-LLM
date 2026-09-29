WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_ADD(commissioned_on, 30) AS commissioned_on_plus_30d
    FROM sites
    WHERE installed_kw <= 386.6
      AND tariff IN ('EV', 'FLAT')
),
zone_stats AS (
    -- Max installed_kw per zone
    SELECT zone, MAX(installed_kw) AS max_installed_kw
    FROM scoped
    GROUP BY zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_installed_kw,
        ROUND(s.installed_kw - g.max_installed_kw, 2) AS installed_kw_minus_max
    FROM scoped s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.installed_kw = g.max_installed_kw
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_name,
        s.commissioned_on,
        s.commissioned_on_plus_30d,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at
    FROM outliers s
    LEFT JOIN meter_readings m ON m.site_id = s.id
)
SELECT
    id,
    site_name,
    commissioned_on_plus_30d,
    reading_kwh,
    reading_voltage,
    reading_read_at
FROM detail_rows_readings
ORDER BY reading_read_at
