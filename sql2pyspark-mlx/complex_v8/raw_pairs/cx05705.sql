WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'OK'
),
latest_per_key AS (
    -- Keep the latest row per site_id, quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY voltage DESC, id DESC) AS recency_rank
        FROM prepared_readings
    ) ranked
    WHERE recency_rank = 1
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MIN(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_voltage,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM latest_per_key s
),
latest AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        reading_count,
        min_reading_voltage,
        first_reading_read_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS row_num
        FROM profiled
    ) ranked
    WHERE row_num = 1
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM latest s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
),
unique_rows AS (
    -- Keep the earliest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        reading_count,
        min_reading_voltage,
        first_reading_read_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY kwh, id) AS recency_rank
        FROM with_activity
    ) ranked
    WHERE recency_rank = 1
),
joined AS (
    -- Join sites
    SELECT
        s.*,
        si.zone AS site_zone,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM unique_rows s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on < DATE '2024-11-21'
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    quality,
    reading_count,
    min_reading_voltage,
    site_name,
    site_commissioned_on,
    CONCAT(site_name, '#', CAST(id AS STRING)) AS site_name_ref
FROM joined
ORDER BY read_at DESC NULLS FIRST, id
LIMIT 20
