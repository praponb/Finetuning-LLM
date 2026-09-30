WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        MONTH(read_at) AS read_at_month
    FROM meter_readings
    WHERE kwh >= 33.9
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-03-08 09:00:00'
        )
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY site_id ORDER BY kwh) AS kwh_rank,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY kwh DESC, id) AS kwh_seq,
        LEAD(read_at, 2) OVER (PARTITION BY site_id ORDER BY kwh, id) AS next2_read_at
    FROM qualified
    WINDOW w_site_id AS (PARTITION BY site_id)
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM ranked s
)
SELECT
    id,
    site_id,
    quality,
    read_at_month,
    rows_in_site_id,
    kwh_seq,
    next2_read_at,
    last_reading_read_at,
    reading_count,
    DATE_TRUNC('WEEK', next2_read_at) AS next2_read_at_week,
    DATE_TRUNC('QUARTER', last_reading_read_at) AS last_reading_read_at_quarter
FROM with_stats
WHERE quality = 'OK'
ORDER BY rows_in_site_id DESC NULLS FIRST
