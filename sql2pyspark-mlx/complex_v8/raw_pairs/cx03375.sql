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
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN zone IN ('Z2', 'Z4') THEN 'group_a' WHEN zone = 'Z3' THEN 'group_b' ELSE 'other' END AS zone_group,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        FLOOR(energy_cost / 250) * 250 AS energy_cost_bucket
    FROM matched
)
-- Subtotals with CUBE
SELECT
    zone_group,
    site_id,
    SUM(energy_cost) FILTER (WHERE quality IS NOT NULL) AS energy_cost_quality_is_not_null,
    SUM(energy_cost_bucket) AS total_energy_cost_bucket
FROM annotated
GROUP BY CUBE(zone_group, site_id)
ORDER BY zone_group NULLS LAST, site_id NULLS LAST
