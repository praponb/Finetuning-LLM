WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        kwh - voltage AS kwh_minus_voltage,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM meter_readings
    WHERE voltage >= 218.4
      AND kwh <= 79.4
),
per_group AS (
    -- Group-level totals
    SELECT
        site_id,
        quality,
        CASE WHEN voltage >= 232.4 THEN 'A' WHEN voltage >= 223.3 THEN 'B' ELSE 'C' END AS voltage_tier,
        ROUND(SUM(kwh_minus_voltage), 2) AS total_kwh_minus_voltage,
        MIN(voltage) AS min_voltage,
        MAX(kwh_minus_voltage) AS max_kwh_minus_voltage,
        ROUND(STDDEV(kwh), 2) AS stddev_kwh
    FROM valid
    GROUP BY site_id, quality, CASE WHEN voltage >= 232.4 THEN 'A' WHEN voltage >= 223.3 THEN 'B' ELSE 'C' END
)
SELECT *
FROM (
    SELECT site_id, voltage_tier, stddev_kwh
    FROM per_group
) src
PIVOT (
    SUM(stddev_kwh)
    FOR voltage_tier IN ('C', 'B')
)
ORDER BY site_id
