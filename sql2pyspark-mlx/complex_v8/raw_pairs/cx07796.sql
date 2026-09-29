WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND(voltage * 1.07, 2) AS voltage_adj
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-02 09:00:00'
      AND voltage BETWEEN 215.6 AND 237.8
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM prepared_readings
    WHERE kwh > (SELECT MIN(kwh) FROM prepared_readings)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        MIN(read_at) AS first_read_at,
        MAX(read_at) AS last_read_at,
        ROUND(SUM(kwh) / NULLIF(SUM(voltage_adj), 0), 3) AS kwh_to_voltage_adj_ratio
    FROM with_activity
    GROUP BY 1, 2
    HAVING MAX(voltage_adj) > 273.0
),
enriched AS (
    -- Label and score rows
    SELECT
        site_id,
        read_at_month,
        first_read_at,
        last_read_at,
        kwh_to_voltage_adj_ratio,
        TO_DATE(last_read_at) AS last_read_at_date,
        FLOOR(kwh_to_voltage_adj_ratio / 2) * 2 AS kwh_to_voltage_adj_ratio_bucket
    FROM totals
    WHERE site_id >= 9
)
-- Final projection
SELECT
    site_id,
    read_at_month,
    first_read_at,
    last_read_at,
    kwh_to_voltage_adj_ratio,
    kwh_to_voltage_adj_ratio_bucket,
    CASE WHEN kwh_to_voltage_adj_ratio >= 5.5 THEN 'HIGH' WHEN kwh_to_voltage_adj_ratio >= 3.5 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_to_voltage_adj_ratio_tier,
    ROUND(kwh_to_voltage_adj_ratio * 1.5, 2) AS kwh_to_voltage_adj_ratio_adj
FROM enriched
ORDER BY first_read_at NULLS LAST, kwh_to_voltage_adj_ratio_adj
