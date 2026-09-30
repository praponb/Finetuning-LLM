WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage + kwh AS voltage_plus_kwh,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE voltage < 229.1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage_plus_kwh,
        voltage_per_kwh,
        MAX(voltage_plus_kwh) OVER w_read_at_month AS read_at_month_max_voltage_plus_kwh,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY MONTH(read_at))) AS distinct_quality_in_read_at_month,
        RANK() OVER (PARTITION BY site_id ORDER BY read_at) AS read_at_rank,
        LAG(voltage, 3) OVER (ORDER BY kwh, id) AS prev3_voltage,
        NTILE(3) OVER (ORDER BY kwh, id) AS kwh_tercile,
        FIRST_VALUE(voltage_plus_kwh) IGNORE NULLS OVER (PARTITION BY quality, site_id ORDER BY voltage_per_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_plus_kwh,
        voltage_per_kwh - LAG(voltage_per_kwh) OVER w_site_id_by_voltage_plus_kwh AS voltage_per_kwh_change,
        MAX(voltage_per_kwh) OVER (PARTITION BY quality) AS quality_max_voltage_per_kwh,
        COUNT(*) OVER w_quality AS rows_in_quality
    FROM filtered
    WINDOW w_read_at_month AS (PARTITION BY MONTH(read_at)), w_site_id_by_voltage_plus_kwh AS (PARTITION BY site_id ORDER BY voltage_plus_kwh, id), w_quality AS (PARTITION BY quality)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh_tercile ASC), 3) AS kwh_tercile_percent_rank,
        MAX(kwh) OVER (PARTITION BY kwh_tercile) AS kwh_tercile_max_kwh,
        LEAD(rows_in_quality, 1, 0) OVER (PARTITION BY site_id ORDER BY read_at_rank ASC, id) AS next_rows_in_quality,
        MIN(quality_max_voltage_per_kwh) OVER (PARTITION BY distinct_quality_in_read_at_month) AS distinct_quality_in_read_at_month_min_quality_ma,
        LEAD(voltage_plus_kwh) OVER w_site_id_by_read_at_rank AS next_voltage_plus_kwh
    FROM with_trends
    WINDOW w_site_id_by_read_at_rank AS (PARTITION BY site_id ORDER BY read_at_rank ASC, id)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    voltage_plus_kwh,
    read_at_month_max_voltage_plus_kwh,
    distinct_quality_in_read_at_month,
    read_at_rank,
    prev3_voltage,
    kwh_tercile,
    voltage_per_kwh_change,
    quality_max_voltage_per_kwh,
    rows_in_quality,
    kwh_tercile_percent_rank,
    kwh_tercile_max_kwh,
    distinct_quality_in_read_at_month_min_quality_ma,
    next_voltage_plus_kwh
FROM sequenced
ORDER BY voltage NULLS LAST, site_id DESC
