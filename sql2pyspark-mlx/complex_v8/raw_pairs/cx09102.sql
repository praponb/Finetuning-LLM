WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE (voltage < 241.8 OR TO_DATE(read_at) < DATE '2025-02-18')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_date,
        ROUND(CUME_DIST() OVER w_all_by_kwh_ties, 3) AS kwh_cume_dist,
        LAG(voltage, 3, 0) OVER w_quality_by_kwh AS prev3_voltage,
        COUNT(*) OVER w_quality_quality_group AS rows_in_quality_quality_group,
        MAX(voltage) OVER (PARTITION BY quality, quality_group) AS quality_quality_group_max_voltage
    FROM recent_readings
    WINDOW w_all_by_kwh_ties AS (ORDER BY kwh ASC), w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh NULLS LAST, id), w_quality_quality_group AS (PARTITION BY quality, quality_group)
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(m.kwh), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_kwh
    FROM windowed s
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh >= 34.6
        )
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    read_at_date,
    prev3_voltage,
    rows_in_quality_quality_group,
    quality_quality_group_max_voltage,
    avg_reading_kwh,
    LEAST(kwh_cume_dist, voltage) AS least_kwh_cume_dist_voltage
FROM without_matches
ORDER BY quality_quality_group_max_voltage DESC, avg_reading_kwh
