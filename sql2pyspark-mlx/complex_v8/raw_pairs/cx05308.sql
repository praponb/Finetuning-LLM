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
site_id_stats AS (
    -- Max voltage per site_id
    SELECT site_id, MAX(voltage) AS max_voltage
    FROM matched
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_voltage,
        ROUND(s.voltage - g.max_voltage, 2) AS voltage_minus_max
    FROM matched s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage = g.max_voltage
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    rate,
    energy_cost,
    max_voltage,
    voltage_minus_max
FROM above_average
WHERE site_id <= 3
