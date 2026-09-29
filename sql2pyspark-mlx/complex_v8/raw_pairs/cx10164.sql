WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE kwh < 89.8
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(voltage, 0.0) AS voltage_filled,
        DATE_FORMAT(read_at_date, 'yyyy-MM-dd') AS read_at_date_label,
        ABS(voltage - 231.7) AS voltage_distance
    FROM filtered_readings
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        kwh,
        quality,
        voltage_filled,
        voltage_distance,
        SUM(voltage) OVER (PARTITION BY quality, site_id) AS quality_site_id_sum_voltage,
        NTH_VALUE(read_at_date, 2) OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_date,
        LAST_VALUE(voltage_filled) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage_filled,
        SUM(kwh) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_sum_4
    FROM derived
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.voltage_distance,
        s.second_read_at_date,
        s.kwh_sum_4,
        si.zone AS site_zone
    FROM metrics s
    JOIN sites si ON si.id = s.site_id
)
SELECT id, field, amount
FROM (
    SELECT id, voltage_distance, kwh_sum_4
    FROM with_parent_site
) src
UNPIVOT (
    amount FOR field IN (voltage_distance, kwh_sum_4)
) unp
ORDER BY id, field
