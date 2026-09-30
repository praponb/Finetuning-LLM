WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 104.8 THEN 'HIGH' WHEN kwh >= 35.9 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        ABS(kwh - 42.1) AS kwh_distance
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 08:00:00'
      AND kwh BETWEEN 20.4 AND 84.9
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage BETWEEN 218.5 AND 240.9
        )
      AND s.voltage >= 219.6
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.kwh_tier,
        s.kwh_distance,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM qualified s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM with_children s
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_stats
    WHERE reading_voltage > (SELECT AVG(reading_voltage) FROM with_stats)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        kwh,
        quality,
        kwh_distance,
        reading_id,
        reading_read_at,
        reading_kwh,
        reading_quality,
        reading_voltage,
        first_reading_read_at,
        LAG(kwh) OVER w_site_id_by_reading_voltage AS prev_kwh,
        ROUND(kwh - AVG(kwh) OVER w_kwh_tier, 2) AS kwh_vs_kwh_tier_avg
    FROM screened
    WINDOW w_site_id_by_reading_voltage AS (PARTITION BY site_id ORDER BY reading_voltage, id, reading_id), w_kwh_tier AS (PARTITION BY kwh_tier)
)
SELECT
    COALESCE(reading_quality, 'ALL') AS reading_quality,
    reading_id,
    COALESCE(quality, 'ALL') AS quality,
    MIN(kwh_vs_kwh_tier_avg) AS min_kwh_vs_kwh_tier_avg,
    MAX(reading_kwh) AS max_reading_kwh,
    SUM(reading_voltage) FILTER (WHERE reading_quality IN ('BAD', 'OK')) AS reading_voltage_reading_quality_in_bad_ok
FROM metrics
GROUP BY ROLLUP(reading_quality, reading_id, quality)
