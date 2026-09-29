WITH in_window AS (
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
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        MAX(read_at) AS last_reading_read_at_2,
        MAX(read_at) AS last_reading_read_at_3
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        m.last_reading_read_at_2,
        m.last_reading_read_at_3
    FROM in_window s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, SUM(kwh) AS total_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.total_reading_kwh
    FROM summarized_rows s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    quality,
    energy_cost,
    last_reading_read_at,
    last_reading_read_at_2,
    last_reading_read_at_3,
    reading_count,
    HOUR(last_reading_read_at_2) AS last_reading_read_at_2_hour
FROM with_counts
ORDER BY last_reading_read_at_2 DESC NULLS FIRST, last_reading_read_at_2_hour NULLS LAST
