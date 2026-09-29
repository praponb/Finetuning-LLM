WITH matched AS (
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
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown,
        ROUND(rate * 1.5, 2) AS rate_adj,
        CASE WHEN zone IN ('Z4', 'Z3') THEN 'group_a' WHEN zone = 'Z2' THEN 'group_b' ELSE 'other' END AS zone_group
    FROM matched
    WHERE energy_cost BETWEEN 270.0 AND 585.0
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        s.zone_or_unknown,
        s.rate_adj,
        s.zone_group,
        si.commissioned_on AS site_commissioned_on
    FROM labeled s
    LEFT JOIN sites si ON si.id = s.site_id
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        s.zone_group,
        si.commissioned_on AS site_commissioned_on_2,
        si.site_name
    FROM with_details_site s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-05-07'
)
SELECT
    id,
    site_id,
    read_at,
    zone,
    site_name,
    ROUND(energy_cost / NULLIF(voltage, 0), 3) AS energy_cost_per_voltage
FROM joined_site
ORDER BY read_at NULLS LAST, site_name DESC
