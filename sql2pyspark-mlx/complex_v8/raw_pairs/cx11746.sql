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
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
        INITCAP(TRIM(zone)) AS zone_clean,
        ROUND(energy_cost * 1.5, 2) AS energy_cost_adj
    FROM covered
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.zone,
        s.rate,
        s.energy_cost,
        s.quality_group,
        s.zone_clean,
        s.energy_cost_adj,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM labeled s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with CUBE
SELECT
    site_id,
    quality_group,
    zone,
    MIN(site_commissioned_on) AS first_site_commissioned_on,
    SUM(kwh) AS total_kwh
FROM joined_site
GROUP BY CUBE(site_id, quality_group, zone)
ORDER BY site_id NULLS LAST, quality_group NULLS LAST, zone NULLS LAST
