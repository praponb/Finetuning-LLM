WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh >= 14.5
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM filtered s
    JOIN meter_readings m ON m.site_id = s.site_id AND MONTH(m.read_at) IN (1, 2)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(kwh) OVER (PARTITION BY reading_id) AS reading_id_sum_kwh,
        ROUND(PERCENT_RANK() OVER w_reading_id_by_reading_read_a, 3) AS reading_read_at_percent_rank
    FROM exploded_detail
    WINDOW w_reading_id_by_reading_read_a AS (PARTITION BY reading_id ORDER BY reading_read_at DESC, id, reading_id)
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.reading_id,
        s.reading_voltage,
        s.reading_read_at,
        s.reading_kwh,
        s.reading_id_sum_kwh,
        s.reading_read_at_percent_rank,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM windowed s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on < DATE '2025-05-10'
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(kwh, 0.0) AS kwh_filled,
        DATE_TRUNC('YEAR', site_commissioned_on) AS site_commissioned_on_year
    FROM with_details
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.reading_id,
        s.reading_read_at_percent_rank,
        s.site_zone,
        s.kwh_filled,
        s.site_commissioned_on_year,
        m.id AS reading_id_2,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at_2,
        m.kwh AS reading_kwh_2
    FROM enriched s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    reading_read_at_percent_rank,
    site_zone,
    kwh_filled,
    site_commissioned_on_year,
    reading_id_2,
    reading_quality,
    reading_read_at_2
FROM line_level
ORDER BY site_zone
