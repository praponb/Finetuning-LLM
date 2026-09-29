WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE quality IN ('EST', 'OK')
),
unique_rows AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh, id) AS recency_rank
        FROM valid_readings
    ) ranked
    WHERE recency_rank = 1
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM unique_rows s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('DAY', read_at) AS read_at_day,
        read_at + INTERVAL 7 DAYS AS read_at_plus_7d
    FROM with_stats
    WHERE site_id >= 8
)
SELECT
    reading_count,
    site_id,
    GROUPING(reading_count) AS is_total_reading_count,
    MAX(read_at_plus_7d) AS last_read_at_plus_7d
FROM enriched
GROUP BY CUBE(reading_count, site_id)
ORDER BY reading_count NULLS LAST, site_id NULLS LAST
