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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
quality_stats AS (
    -- Max rate per quality
    SELECT quality, MAX(rate) AS max_rate
    FROM covered
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_rate, ROUND(s.rate - g.max_rate, 2) AS rate_minus_max
    FROM covered s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.rate = g.max_rate
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.rate,
        s.energy_cost,
        s.rate_minus_max,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM above_average s
    JOIN sites si ON si.id = s.site_id
),
with_parent_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.energy_cost,
        s.site_zone,
        si.site_name
    FROM enriched_site s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    energy_cost,
    site_zone,
    site_name
FROM with_parent_site
ORDER BY read_at DESC, energy_cost DESC, id
LIMIT 5
