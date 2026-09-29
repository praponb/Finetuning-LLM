WITH joined_ranges AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.rate,
        si.installed_kw AS site_installed_kw,
        si.site_name,
        si.zone AS site_zone
    FROM joined_ranges s
    LEFT JOIN sites si ON si.id = s.site_id
),
site_id_stats AS (
    -- Avg voltage per site_id
    SELECT site_id, AVG(voltage) AS avg_voltage
    FROM combined_details
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM combined_details s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage > g.avg_voltage
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(site_zone, 'ALL') AS site_zone,
    site_id,
    GROUPING_ID() AS gid,
    MAX(read_at) AS last_read_at,
    SUM(CASE WHEN site_name IS NOT NULL THEN 1 ELSE 0 END) AS n_site_name_is_not_null,
    ROUND(STDDEV(voltage), 2) AS stddev_voltage
FROM above_average
GROUP BY ROLLUP(site_zone, site_id)
ORDER BY site_zone NULLS LAST, site_id NULLS LAST
