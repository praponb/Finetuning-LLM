WITH joined_ranges AS (
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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE m.quality = 'OK') FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh
    FROM joined_ranges s
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(kwh) AS min_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_kwh
    FROM profiled s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.rate,
        s.energy_cost,
        s.total_reading_kwh,
        s.min_reading_kwh,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM summarized_rows s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    kwh,
    rate,
    energy_cost,
    total_reading_kwh,
    min_reading_kwh,
    reading_id,
    reading_voltage,
    reading_kwh,
    COALESCE(kwh, 0.0) AS kwh_filled,
    reading_kwh - min_reading_kwh AS reading_kwh_minus_min_reading_kwh
FROM detail_rows_readings
WHERE site_id >= 8
