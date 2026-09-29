WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage - kwh AS voltage_minus_kwh,
        ROUND(voltage * 0.9, 2) AS voltage_adj
    FROM meter_readings
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM base_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        FLOOR(voltage_adj / 25) * 25 AS voltage_adj_bucket,
        ABS(kwh - 51.0) AS kwh_distance
    FROM without_matches
    WHERE voltage_adj >= 273.0
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY voltage_minus_kwh ASC NULLS LAST, id) AS rn,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY MONTH(read_at), site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        NTILE(3) OVER (PARTITION BY TO_DATE(read_at), site_id ORDER BY read_at DESC, id) AS read_at_tercile,
        MAX(days_since_read_at) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)) AS read_at_month_max_days_since_read_at,
        MAX(voltage_adj) OVER w_read_at_month AS read_at_month_max_voltage_adj
    FROM tagged
    WINDOW w_read_at_month AS (PARTITION BY DATE_TRUNC('MONTH', read_at))
)
-- Subtotals with CUBE
SELECT
    read_at_tercile,
    rn,
    MIN(first_known_kwh) AS min_first_known_kwh,
    PERCENTILE_APPROX(kwh_distance, 0.5) AS median_kwh_distance,
    MAX(read_at) AS last_read_at
FROM with_windows
GROUP BY CUBE(read_at_tercile, rn)
ORDER BY read_at_tercile NULLS LAST, rn NULLS LAST
