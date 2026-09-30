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
site_id_stats AS (
    -- Avg voltage per site_id
    SELECT site_id, AVG(voltage) AS avg_voltage
    FROM covered
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_voltage,
        ROUND(s.voltage - g.avg_voltage, 2) AS voltage_minus_avg
    FROM covered s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage > g.avg_voltage
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
    avg_voltage,
    voltage_minus_avg
FROM above_average
WHERE rate <= 7.6
ORDER BY avg_voltage DESC, voltage_minus_avg, id
LIMIT 20
