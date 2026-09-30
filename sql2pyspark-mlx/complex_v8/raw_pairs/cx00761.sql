WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE kwh >= 20.4
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        quality,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(kwh, 0.5) AS median_kwh,
        ROUND(SUM(kwh) / NULLIF(SUM(voltage), 0), 3) AS kwh_to_voltage_ratio,
        MIN(read_at) AS first_read_at,
        MAX(read_at) AS last_read_at
    FROM cleaned
    WHERE site_id IS NOT NULL
    GROUP BY quality
    HAVING COUNT(*) >= 2
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE median_kwh > (SELECT AVG(median_kwh) FROM stats)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(row_count) OVER (ORDER BY last_read_at, quality ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS row_count_sum_4,
        COUNT(*) OVER w_row_count_quality AS rows_in_row_count_quality,
        MIN(kwh_to_voltage_ratio) OVER (PARTITION BY row_count, quality) AS row_count_quality_min_kwh_to_voltage_ratio,
        kwh_to_voltage_ratio - LAG(kwh_to_voltage_ratio) OVER (PARTITION BY quality ORDER BY kwh_to_voltage_ratio, quality) AS kwh_to_voltage_ratio_change
    FROM screened
    WINDOW w_row_count_quality AS (PARTITION BY row_count, quality)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY row_count)) AS distinct_quality_in_row_count,
        COUNT(*) OVER w_row_count AS rows_in_row_count
    FROM with_windows
    WINDOW w_row_count AS (PARTITION BY row_count)
)
SELECT *
FROM (
    SELECT first_read_at, quality, kwh_to_voltage_ratio
    FROM metrics
) src
PIVOT (
    AVG(kwh_to_voltage_ratio)
    FOR quality IN ('BAD', 'OK', 'EST')
)
