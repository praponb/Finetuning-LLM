WITH covered AS (
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
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        si.site_name,
        si.tariff AS site_tariff
    FROM covered s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    zone,
    rate,
    energy_cost,
    site_name,
    site_tariff,
    DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label,
    DATE_TRUNC('WEEK', read_at) AS read_at_week
FROM enriched_site
ORDER BY read_at_label, site_id DESC NULLS FIRST
