WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-24 10:00:00'
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage
    FROM filtered_readings s
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        read_at_label,
        max_reading_voltage,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_1_prior,
        MAX(max_reading_voltage) OVER (PARTITION BY site_id) AS site_id_max_max_reading_voltage
    FROM with_stats
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    read_at_label,
    max_reading_voltage,
    kwh_avg_1_prior,
    site_id_max_max_reading_voltage,
    CONCAT(read_at_label, '#', CAST(site_id AS STRING)) AS read_at_label_ref,
    FLOOR(kwh_avg_1_prior / 25) * 25 AS kwh_avg_1_prior_bucket
FROM with_windows
WHERE site_id <= 4
ORDER BY site_id, id
LIMIT 20
