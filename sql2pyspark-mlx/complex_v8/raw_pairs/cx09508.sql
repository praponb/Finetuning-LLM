WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ABS(voltage - 221.8) AS voltage_distance,
        LEAST(kwh, voltage) AS least_kwh_voltage,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE site_id <= 7
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        voltage_distance,
        least_kwh_voltage,
        voltage_per_kwh,
        COUNT(*) OVER w_quality AS rows_in_quality,
        ROW_NUMBER() OVER w_all_by_least_kwh_voltage AS seq_no,
        SUM(voltage_distance) OVER w_quality AS quality_sum_voltage_distance,
        ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage_distance DESC, id) AS voltage_distance_seq
    FROM base_readings
    WINDOW w_quality AS (PARTITION BY quality), w_all_by_least_kwh_voltage AS (ORDER BY least_kwh_voltage DESC, id)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.voltage_per_kwh,
        s.rows_in_quality,
        s.seq_no,
        s.quality_sum_voltage_distance,
        si.zone AS site_zone
    FROM with_windows s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT seq_no, quality, site_zone, rows_in_quality
    FROM with_parent
) src
PIVOT (
    SUM(rows_in_quality) AS amount, MAX(rows_in_quality) AS max_v
    FOR site_zone IN ('Z3', 'Z4')
)
