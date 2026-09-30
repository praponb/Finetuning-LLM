WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-12 12:00:00'
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        voltage,
        quality_or_unknown,
        LEAST(kwh, voltage) AS least_kwh_voltage
    FROM valid
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.least_kwh_voltage,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM derived s
    JOIN meter_readings m ON m.site_id = s.site_id
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.least_kwh_voltage,
        s.reading_id,
        s.reading_quality,
        m.id AS reading_id_2,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage_2
    FROM with_children s
    JOIN meter_readings m ON m.site_id = s.site_id AND MONTH(m.read_at) IN (1, 3)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(reading_voltage_2) OVER (ORDER BY reading_read_at, id, reading_id, reading_id_2) AS prev_reading_voltage_2,
        FIRST_VALUE(reading_read_at) OVER (ORDER BY reading_read_at, id, reading_id, reading_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reading_read_at,
        ROUND(reading_voltage_2 - AVG(reading_voltage_2) OVER (PARTITION BY reading_quality), 2) AS reading_voltage_2_vs_reading_quality_avg,
        ROW_NUMBER() OVER (ORDER BY reading_read_at, id, reading_id, reading_id_2) AS reading_read_at_seq,
        ROUND(PERCENT_RANK() OVER w_all_by_reading_read_at_ties, 3) AS reading_read_at_percent_rank
    FROM detail_rows_readings
    WINDOW w_all_by_reading_read_at_ties AS (ORDER BY reading_read_at)
)
-- Final projection
SELECT
    id,
    site_id,
    least_kwh_voltage,
    reading_id,
    reading_quality,
    reading_id_2,
    first_reading_read_at,
    reading_voltage_2_vs_reading_quality_avg
FROM windowed
ORDER BY reading_id DESC
