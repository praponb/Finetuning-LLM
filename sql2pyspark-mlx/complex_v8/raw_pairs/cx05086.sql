WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.5, 2) AS kwh_adj
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-02-24'
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM filtered s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.quality IN ('EST', 'OK')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(read_at, 2) OVER (ORDER BY read_at, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        RANK() OVER (ORDER BY read_at) AS read_at_rank,
        FIRST_VALUE(reading_read_at) OVER (ORDER BY read_at, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reading_read_at,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_reading_quality_site_id, 0), 4) AS kwh_share_of_reading_quality_site_id,
        LAST_VALUE(reading_read_at) OVER (ORDER BY read_at, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reading_read_at
    FROM line_level_readings
    WINDOW w_reading_quality_site_id AS (PARTITION BY reading_quality, site_id)
),
with_details_reading AS (
    -- Attach reading attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.reading_id,
        s.reading_read_at,
        s.second_read_at,
        s.read_at_rank,
        s.first_reading_read_at,
        s.kwh_share_of_reading_quality_site_id,
        s.last_reading_read_at,
        m.site_id AS reading_site_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage
    FROM metrics s
    LEFT JOIN meter_readings m ON m.id = s.reading_id
),
enriched_reading AS (
    -- Attach reading attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.reading_id,
        s.reading_read_at,
        s.second_read_at,
        s.read_at_rank,
        s.first_reading_read_at,
        s.kwh_share_of_reading_quality_site_id,
        s.last_reading_read_at,
        s.reading_site_id,
        s.reading_kwh,
        s.reading_voltage,
        m.read_at AS reading_read_at_2
    FROM with_details_reading s
    JOIN meter_readings m ON m.id = s.reading_id AND m.quality = 'BAD'
)
-- Final projection
SELECT
    id,
    site_id,
    reading_read_at,
    second_read_at,
    read_at_rank,
    first_reading_read_at,
    kwh_share_of_reading_quality_site_id,
    last_reading_read_at,
    reading_kwh,
    reading_read_at_2,
    DATE_FORMAT(reading_read_at, 'yyyy-MM-dd') AS reading_read_at_label,
    reading_kwh + reading_voltage AS reading_kwh_plus_reading_voltage
FROM enriched_reading
ORDER BY first_reading_read_at
