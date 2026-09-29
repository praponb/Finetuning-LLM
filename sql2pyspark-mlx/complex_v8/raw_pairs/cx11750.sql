WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
    WHERE kwh > 43.1
),
latest_per_key AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_len,
        voltage_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at DESC, id DESC) AS rn
        FROM valid_readings
    ) ranked
    WHERE rn = 1
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        SUM(voltage) AS total_reading_voltage,
        MIN(read_at) AS first_reading_read_at,
        MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.total_reading_voltage,
        m.first_reading_read_at,
        m.last_reading_read_at
    FROM latest_per_key s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.voltage_bucket,
        s.total_reading_voltage,
        s.first_reading_read_at,
        si.commissioned_on AS site_commissioned_on
    FROM with_stats s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    first_reading_read_at
FROM joined_site
ORDER BY voltage DESC, first_reading_read_at DESC NULLS FIRST
