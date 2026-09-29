WITH in_window AS (
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
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(rate / NULLIF(energy_cost, 0), 3) AS rate_per_energy_cost,
        CONCAT(zone, '#', CAST(site_id AS STRING)) AS zone_ref
    FROM in_window
    WHERE read_at < TIMESTAMP '2025-02-12 08:00:00'
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    zone,
    rate,
    energy_cost,
    zone_ref,
    MONTH(read_at) AS read_at_month
FROM scored
ORDER BY quality, site_id
