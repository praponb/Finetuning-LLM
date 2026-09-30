WITH joined_ranges AS (
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
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(m.voltage), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_voltage,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM joined_ranges s
),
site_id_stats AS (
    -- Median rate per site_id
    SELECT site_id, PERCENTILE_APPROX(rate, 0.5) AS median_rate
    FROM with_stats
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_rate, ROUND(s.rate - g.median_rate, 2) AS rate_minus_median
    FROM with_stats s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.rate >= g.median_rate
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    zone,
    rate,
    energy_cost,
    avg_reading_voltage,
    rate_minus_median,
    SUBSTRING(TRIM(zone), 1, 2) AS zone_prefix,
    LEAST(voltage, median_rate) AS least_voltage_median_rate
FROM vs_group
WHERE first_reading_read_at < TIMESTAMP '2025-02-05 09:00:00'
ORDER BY kwh
