WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label
    FROM meter_readings
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(read_at_hour, 'yyyy-MM') AS read_at_hour_ym,
        YEAR(read_at) AS read_at_year,
        MIN(voltage) AS min_voltage,
        ROUND(STDDEV(kwh), 2) AS stddev_kwh,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT site_id) AS distinct_site_ids
    FROM valid_readings
    WHERE kwh >= 35.0
    GROUP BY DATE_FORMAT(read_at_hour, 'yyyy-MM'), YEAR(read_at)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        read_at_year - min_voltage AS read_at_year_minus_min_voltage,
        ROUND(stddev_kwh * 0.9, 2) AS stddev_kwh_adj,
        COALESCE(min_voltage, 0.0) AS min_voltage_filled
    FROM stats
    WHERE stddev_kwh <= 108.0
)
-- Final projection
SELECT
    read_at_hour_ym,
    min_voltage,
    stddev_kwh,
    n_rows,
    distinct_site_ids,
    read_at_year_minus_min_voltage,
    stddev_kwh_adj,
    min_voltage_filled
FROM scored
WHERE n_rows <= 9
ORDER BY stddev_kwh DESC, min_voltage_filled DESC
