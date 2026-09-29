WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage >= 224.8
      AND quality IN ('BAD', 'EST')
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(voltage) AS max_reading_voltage, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.max_reading_voltage,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM valid s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(reading_count / NULLIF(SUM(reading_count) OVER (PARTITION BY reading_count), 0), 4) AS reading_count_share_of_reading_count,
        RANK() OVER (PARTITION BY site_id, reading_count ORDER BY read_at ASC) AS read_at_rank,
        ROW_NUMBER() OVER (PARTITION BY site_id, reading_count ORDER BY read_at ASC, id) AS rn,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY read_at, id) AS rn_2,
        SUM(kwh) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS kwh_sum_3_prior
    FROM profiled
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.max_reading_voltage,
        s.reading_count_share_of_reading_count,
        s.rn_2,
        s.kwh_sum_3_prior,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage
    FROM ranked s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.kwh <= 96.8
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        max_reading_voltage,
        reading_count_share_of_reading_count,
        rn_2,
        kwh_sum_3_prior,
        reading_id,
        reading_kwh,
        ROUND(AVG(kwh_sum_3_prior) OVER (PARTITION BY site_id ORDER BY voltage DESC, id, reading_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_sum_3_prior_avg_3_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY voltage DESC), 3) AS voltage_cume_dist
    FROM line_level_readings
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage_2
    FROM metrics s
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    max_reading_voltage,
    reading_count_share_of_reading_count,
    rn_2,
    kwh_sum_3_prior,
    reading_id,
    kwh_sum_3_prior_avg_3_prior,
    voltage_cume_dist,
    reading_count
FROM with_stats
ORDER BY kwh
