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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM matched s
    JOIN sites si ON si.id = s.site_id
),
combined_details_site AS (
    -- Attach site attributes
    SELECT s.*, si.zone AS site_zone_2
    FROM joined s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone NOT IN ('Z3', 'Z2')
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        rate,
        site_commissioned_on,
        site_zone,
        site_zone_2,
        LEAST(energy_cost, voltage) AS least_energy_cost_voltage,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM combined_details_site
)
-- Final projection
SELECT
    id,
    voltage,
    rate,
    site_commissioned_on,
    site_zone,
    site_zone_2,
    kwh_bucket
FROM enriched
WHERE least_energy_cost_voltage < 540.0
ORDER BY site_commissioned_on DESC, site_zone DESC NULLS FIRST
