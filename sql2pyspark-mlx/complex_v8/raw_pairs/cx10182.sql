WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage <= 232.0
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        SUM(voltage) OVER w_read_at_month AS read_at_month_sum_voltage,
        FIRST_VALUE(voltage) OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        MAX(voltage) OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        LAST_VALUE(read_at) OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at
    FROM cleaned
    WINDOW w_read_at_month AS (PARTITION BY MONTH(read_at))
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN first_voltage >= 240.1 THEN 'A' WHEN first_voltage >= 222.1 THEN 'B' ELSE 'C' END AS first_voltage_tier,
        site_id,
        quality,
        ROUND(SUM(max_voltage_so_far), 2) AS total_max_voltage_so_far,
        MAX(read_at) AS last_read_at_2,
        ROUND(SUM(first_voltage) / NULLIF(SUM(read_at_month_sum_voltage), 0), 3) AS first_voltage_to_read_at_month_sum_voltage_ratio
    FROM with_trends
    WHERE HOUR(read_at) BETWEEN 8 AND 13
    GROUP BY 1, 2, 3
    HAVING AVG(read_at_month_sum_voltage) > 994.0
)
SELECT *
FROM (
    SELECT site_id, first_voltage_tier, total_max_voltage_so_far
    FROM grouped
) src
PIVOT (
    COUNT(total_max_voltage_so_far)
    FOR first_voltage_tier IN ('A', 'B')
)
ORDER BY site_id
