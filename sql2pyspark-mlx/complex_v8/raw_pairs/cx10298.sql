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
zone_stats AS (
    -- Median kwh per zone
    SELECT zone, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM in_window
    GROUP BY zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM in_window s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.kwh >= g.median_kwh
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(read_at) AS read_at_day,
        site_id,
        zone,
        ROUND(SUM(kwh_minus_median) / NULLIF(SUM(kwh), 0), 3) AS kwh_minus_median_to_kwh_ratio,
        MAX(kwh) AS max_kwh,
        ROUND(SUM(voltage) / NULLIF(SUM(kwh), 0), 3) AS voltage_to_kwh_ratio,
        MAX(read_at) AS last_read_at,
        MIN(kwh) AS min_kwh,
        SUM(voltage) FILTER (WHERE zone IN ('Z4', 'Z2')) AS voltage_zone_in_z4_z2
    FROM outliers
    WHERE kwh_minus_median <= 96.0
    GROUP BY 1, 2, 3
    HAVING MAX(kwh_minus_median) > -60.0
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(m.voltage), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_voltage,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM rollup_base s
)
SELECT
    read_at_day,
    site_id,
    kwh_minus_median_to_kwh_ratio,
    max_kwh,
    voltage_to_kwh_ratio,
    last_read_at,
    min_kwh,
    avg_reading_voltage,
    last_reading_read_at,
    LENGTH(TRIM(zone)) AS zone_len,
    GREATEST(max_kwh, voltage_to_kwh_ratio) AS greatest_max_kwh_voltage_to_kwh_ratio
FROM with_counts
ORDER BY site_id
