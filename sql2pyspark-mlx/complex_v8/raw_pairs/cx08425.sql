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
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(read_at) AS first_reading_read_at,
        COUNT(CASE WHEN quality <> 'BAD' THEN 1 END) AS reading_quality_count,
        COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM joined_ranges s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT site_id, ROUND(AVG(voltage), 2) AS avg_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_voltage
    FROM with_counts s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
),
deduped AS (
    -- Keep the latest row per site_id, zone
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        zone,
        rate,
        energy_cost,
        first_reading_read_at,
        reading_quality_count,
        reading_count,
        avg_reading_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, zone ORDER BY read_at DESC, id DESC) AS row_num
        FROM summarized_rows
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    zone,
    rate,
    energy_cost,
    reading_count,
    avg_reading_voltage
FROM deduped
ORDER BY kwh DESC
