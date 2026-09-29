WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND(kwh * 1.5, 2) AS kwh_adj,
        kwh + voltage AS kwh_plus_voltage,
        MONTH(read_at) AS read_at_month
    FROM meter_readings
    WHERE kwh >= 48.2
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh
    FROM filtered_readings s
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM with_counts
    WHERE max_reading_voltage > (SELECT MIN(max_reading_voltage) FROM with_counts)
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 101.2
        )
      AND s.kwh_plus_voltage <= 310.2
)
SELECT
    site_id,
    read_at_month,
    ROUND(STDDEV(kwh_plus_voltage), 2) AS stddev_kwh_plus_voltage,
    ROUND(SUM(read_at_month) / NULLIF(SUM(max_reading_voltage), 0), 3) AS read_at_month_to_max_reading_voltage_ratio
FROM without_matches
GROUP BY ROLLUP(site_id, read_at_month)
ORDER BY site_id NULLS LAST, read_at_month NULLS LAST
