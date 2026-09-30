WITH in_window AS (
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
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM in_window s
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        zone,
        rate,
        reading_count,
        first_reading_read_at,
        ROUND(reading_count / NULLIF(kwh, 0), 3) AS reading_count_per_kwh
    FROM profiled
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    zone,
    rate,
    reading_count,
    first_reading_read_at,
    first_reading_read_at + INTERVAL 2 HOURS AS first_reading_read_at_plus_2h,
    ROUND(voltage * 0.9, 2) AS voltage_adj
FROM tagged
ORDER BY zone, id
LIMIT 10
