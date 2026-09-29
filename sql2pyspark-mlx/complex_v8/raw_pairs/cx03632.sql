WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(voltage, 0.0) AS voltage_filled,
        CASE WHEN kwh >= 104.8 THEN 'A' WHEN kwh >= 30.2 THEN 'B' ELSE 'C' END AS kwh_tier,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-12 08:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_filled,
        kwh_tier,
        COUNT(*) OVER (PARTITION BY site_id, kwh_tier) AS rows_in_site_id_kwh_tier,
        FIRST_VALUE(voltage) OVER (PARTITION BY kwh_tier ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage
    FROM scoped_readings
)
SELECT
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    COALESCE(kwh_tier, 'ALL') AS kwh_tier,
    COUNT(*) AS n_rows
FROM ranked
GROUP BY GROUPING SETS ((quality, site_id, kwh_tier), (quality))
ORDER BY quality NULLS LAST, site_id NULLS LAST, kwh_tier NULLS LAST
