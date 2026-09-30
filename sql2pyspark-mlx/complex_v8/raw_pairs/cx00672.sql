WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean,
        kwh - voltage AS kwh_minus_voltage
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND read_at < TIMESTAMP '2025-02-18 07:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(voltage) OVER w_quality AS quality_sum_voltage,
        COUNT(*) OVER (PARTITION BY quality, YEAR(read_at)) AS rows_in_quality_read_at_year,
        COUNT(*) OVER w_quality AS rows_in_quality,
        ROUND(CUME_DIST() OVER w_all_by_kwh_minus_voltage_tie, 3) AS kwh_minus_voltage_cume_dist,
        MIN(voltage) OVER (PARTITION BY quality, site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        SUM(kwh_minus_voltage) OVER (PARTITION BY quality) AS quality_sum_kwh_minus_voltage,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality, YEAR(read_at)), 2) AS voltage_vs_quality_read_at_year_avg,
        DENSE_RANK() OVER (ORDER BY read_at, id) AS read_at_dense_rank
    FROM prepared_readings
    WINDOW w_quality AS (PARTITION BY quality), w_all_by_kwh_minus_voltage_tie AS (ORDER BY kwh_minus_voltage)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    quality_clean,
    kwh_minus_voltage,
    quality_sum_voltage,
    rows_in_quality_read_at_year,
    rows_in_quality,
    min_voltage_so_far,
    quality_sum_kwh_minus_voltage,
    voltage_vs_quality_read_at_year_avg,
    read_at_dense_rank,
    FLOOR(quality_sum_voltage / 500) * 500 AS quality_sum_voltage_bucket,
    voltage + kwh_minus_voltage AS voltage_plus_kwh_minus_voltage
FROM with_trends
ORDER BY quality_sum_kwh_minus_voltage DESC
