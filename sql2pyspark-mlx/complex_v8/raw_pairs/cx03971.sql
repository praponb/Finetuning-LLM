WITH covered AS (
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
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.zone,
        s.rate,
        s.energy_cost,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM covered s
    LEFT JOIN sites si ON si.id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        ROUND(AVG(voltage), 2) AS avg_reading_voltage,
        COUNT(*) FILTER (WHERE quality = 'OK') AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_voltage,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM with_details s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        DATE_TRUNC('MONTH', site_commissioned_on) AS site_commissioned_on_month,
        COUNT(*) AS cnt,
        ROUND(STDDEV(avg_reading_voltage), 2) AS stddev_avg_reading_voltage
    FROM with_counts
    GROUP BY DATE_TRUNC('MONTH', read_at), DATE_TRUNC('MONTH', site_commissioned_on)
    HAVING COUNT(*) >= 2
)
SELECT read_at_month, cnt, TO_DATE(read_at_month) AS read_at_month_date
FROM per_group
ORDER BY read_at_month_date, read_at_month DESC
