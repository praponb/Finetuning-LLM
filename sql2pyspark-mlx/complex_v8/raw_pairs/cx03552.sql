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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM matched
    WHERE kwh > (SELECT AVG(kwh) FROM matched)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, zone, energy_cost
    FROM screened
) src
PIVOT (
    SUM(energy_cost) AS total, MAX(energy_cost) AS max_v
    FOR zone IN ('Z4', 'Z3')
)
