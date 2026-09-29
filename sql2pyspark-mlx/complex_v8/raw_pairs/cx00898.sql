WITH matched AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
zone_stats AS (
    -- Avg energy_cost per zone
    SELECT zone, AVG(energy_cost) AS avg_energy_cost
    FROM matched
    GROUP BY zone
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_energy_cost,
        ROUND(s.energy_cost - g.avg_energy_cost, 2) AS energy_cost_minus_avg
    FROM matched s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.energy_cost > g.avg_energy_cost
),
joined_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.zone,
        s.rate,
        s.energy_cost,
        s.avg_energy_cost,
        s.energy_cost_minus_avg,
        si.installed_kw AS site_installed_kw
    FROM above_average s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    rate,
    avg_energy_cost,
    site_installed_kw,
    FLOOR(avg_energy_cost / 250) * 250 AS avg_energy_cost_bucket,
    LENGTH(TRIM(zone)) AS zone_len
FROM joined_site
WHERE energy_cost < 540.0
ORDER BY zone_len, avg_energy_cost_bucket DESC
