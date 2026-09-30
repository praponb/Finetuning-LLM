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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        rate,
        energy_cost,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM covered
    WHERE energy_cost < 765.0
)
SELECT id, measure, reading
FROM (
    SELECT id, rate, kwh
    FROM tagged
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (rate, kwh)
) unp
