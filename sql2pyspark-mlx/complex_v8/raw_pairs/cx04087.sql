WITH covered AS (
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
site_id_stats AS (
    -- Avg energy_cost per site_id
    SELECT site_id, AVG(energy_cost) AS avg_energy_cost
    FROM covered
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_energy_cost,
        ROUND(s.energy_cost - g.avg_energy_cost, 2) AS energy_cost_minus_avg
    FROM covered s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.energy_cost > g.avg_energy_cost
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.zone,
        s.energy_cost,
        s.energy_cost_minus_avg,
        si.site_name
    FROM above_average s
    LEFT JOIN sites si ON si.id = s.site_id
),
enriched AS (
    -- Attach site attributes
    SELECT s.*, si.tariff AS site_tariff, si.zone AS site_zone
    FROM joined_site s
    LEFT JOIN sites si ON si.id = s.site_id AND si.installed_kw < 284.0
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    energy_cost,
    site_name,
    site_tariff,
    site_zone,
    CONCAT_WS('|', site_zone, site_tariff) AS site_zone_site_tariff_label
FROM enriched
ORDER BY site_id
