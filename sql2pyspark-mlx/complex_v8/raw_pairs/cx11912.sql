WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage < 233.6
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        si.commissioned_on AS site_commissioned_on
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at
    FROM enriched_site s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(last_reading_read_at) OVER w_site_id_by_last_reading_read AS prev_last_reading_read_at,
        FIRST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY last_reading_read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_commissioned_on,
        LEAD(site_commissioned_on) OVER (PARTITION BY site_id ORDER BY last_reading_read_at DESC, id) AS next_site_commissioned_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY last_reading_read_at DESC, id), 3) AS last_reading_read_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY last_reading_read_at DESC) AS last_reading_read_at_dense_rank
    FROM profiled
    WINDOW w_site_id_by_last_reading_read AS (PARTITION BY site_id ORDER BY last_reading_read_at DESC, id)
)
-- Final projection
SELECT
    id,
    site_id,
    quality,
    site_commissioned_on,
    prev_last_reading_read_at,
    first_known_site_commissioned_on,
    next_site_commissioned_on,
    last_reading_read_at_percent_rank,
    last_reading_read_at_dense_rank,
    SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
FROM with_windows
ORDER BY site_commissioned_on
