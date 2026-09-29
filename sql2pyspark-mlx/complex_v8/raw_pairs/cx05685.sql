WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN voltage >= 238.8 THEN 'large' WHEN voltage >= 220.2 THEN 'medium' ELSE 'small' END AS voltage_tier,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM meter_readings
    WHERE (kwh < 86.0 OR site_id IS NOT NULL)
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM scoped s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.read_at_label,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM with_activity s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_children_readings_2 AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at_label,
        s.reading_id,
        s.reading_voltage,
        s.reading_kwh,
        s.reading_read_at,
        m.id AS reading_id_2,
        m.read_at AS reading_read_at_2,
        m.voltage AS reading_voltage_2
    FROM with_children_readings s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
latest AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at_label,
        reading_id,
        reading_voltage,
        reading_kwh,
        reading_read_at,
        reading_id_2,
        reading_read_at_2,
        reading_voltage_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY reading_voltage_2 DESC, id DESC, reading_id DESC, reading_id_2 DESC) AS rn
        FROM with_children_readings_2
    ) ranked
    WHERE rn = 1
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_kwh,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM latest s
),
latest_per_key AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at_label,
        reading_id,
        reading_voltage,
        reading_kwh,
        reading_read_at,
        reading_id_2,
        reading_read_at_2,
        reading_voltage_2,
        max_reading_kwh,
        reading_count,
        reading_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY reading_count_2, id, reading_id, reading_id_2) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    site_id,
    read_at_label,
    reading_id,
    reading_read_at,
    reading_id_2,
    reading_read_at_2,
    max_reading_kwh,
    reading_count_2,
    CASE WHEN reading_count_2 >= 6 THEN 'large' WHEN reading_count_2 >= 2 THEN 'medium' ELSE 'small' END AS reading_count_2_tier
FROM latest_per_key
ORDER BY reading_id_2 DESC NULLS FIRST, read_at_label
