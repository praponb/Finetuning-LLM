WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled,
        HOUR(read_at) AS read_at_hour
    FROM meter_readings
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        kwh_filled,
        read_at_hour,
        ROUND(AVG(read_at_hour) OVER w_read_at_month_quality, 2) AS read_at_month_quality_avg_read_at_hour,
        MAX(kwh_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality) AS read_at_month_quality_max_kwh_filled,
        SUM(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), quality) AS read_at_month_quality_sum_voltage
    FROM filtered_readings
    WINDOW w_read_at_month_quality AS (PARTITION BY DATE_TRUNC('MONTH', read_at), quality)
)
SELECT id, metric, amount
FROM (
    SELECT id, kwh, read_at_month_quality_avg_read_at_hour, kwh_filled, read_at_month_quality_max_kwh_filled
    FROM metrics
) src
UNPIVOT (
    amount FOR metric IN (kwh, read_at_month_quality_avg_read_at_hour, kwh_filled, read_at_month_quality_max_kwh_filled)
) unp
ORDER BY id, metric
