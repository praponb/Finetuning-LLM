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
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        si.zone AS site_zone
    FROM matched s
    JOIN sites si ON si.id = s.site_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, voltage, kwh, rate
    FROM joined_site
) src
UNPIVOT (
    value FOR metric_name IN (voltage, kwh, rate)
) unp
ORDER BY id, metric_name
