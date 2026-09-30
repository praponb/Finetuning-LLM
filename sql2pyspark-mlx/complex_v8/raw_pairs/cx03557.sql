WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ABS(voltage - 222.6) AS voltage_distance,
        TO_DATE(read_at) AS read_at_date,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM meter_readings
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at_date,
        s.read_at_label,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM recent s
    JOIN meter_readings m ON m.site_id = s.site_id
),
site_id_stats AS (
    -- Max reading_kwh per site_id
    SELECT site_id, MAX(reading_kwh) AS max_reading_kwh
    FROM with_children_readings
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_reading_kwh,
        ROUND(s.reading_kwh - g.max_reading_kwh, 2) AS reading_kwh_minus_max
    FROM with_children_readings s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.reading_kwh = g.max_reading_kwh
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM above_average
    WHERE max_reading_kwh > (SELECT MIN(max_reading_kwh) FROM above_average)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at_label,
        reading_id,
        reading_kwh,
        max_reading_kwh,
        reading_kwh_minus_max,
        CONCAT(read_at_label, '#', CAST(reading_id AS STRING)) AS read_at_label_ref
    FROM screened
)
SELECT
    id,
    reading_id,
    reading_kwh,
    max_reading_kwh,
    reading_kwh_minus_max,
    read_at_label_ref,
    ABS(reading_kwh - 63.4) AS reading_kwh_distance,
    ROUND(max_reading_kwh * 1.1, 2) AS max_reading_kwh_adj
FROM tagged
