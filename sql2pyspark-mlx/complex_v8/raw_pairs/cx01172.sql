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
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(voltage) OVER (PARTITION BY zone, site_id), 2) AS zone_site_id_avg_voltage,
        NTILE(4) OVER (PARTITION BY site_id ORDER BY read_at, id) AS read_at_quartile
    FROM covered
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.rate,
        s.zone_site_id_avg_voltage,
        s.read_at_quartile,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM sequenced s
    JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    rate,
    zone_site_id_avg_voltage,
    read_at_quartile,
    reading_id,
    reading_voltage
FROM detail_rows_readings
ORDER BY rate DESC NULLS FIRST, id, reading_id
LIMIT 5
