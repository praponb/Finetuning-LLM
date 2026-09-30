WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(voltage, 0.0) AS voltage_filled,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 3)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        voltage_filled,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        MAX(voltage_filled) OVER (ORDER BY kwh DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_filled_max_2_prior
    FROM prepared_readings
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(last_known_read_at, 'yyyy-MM') AS last_known_read_at_label,
        CASE WHEN voltage_filled >= 238.0 THEN 'HIGH' WHEN voltage_filled >= 220.5 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_filled_tier,
        ABS(voltage_filled - 225.8) AS voltage_filled_distance
    FROM ranked
    WHERE distinct_quality_in_site_id >= 5
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE voltage_filled > (SELECT AVG(voltage_filled) FROM derived)
)
SELECT id, measure, amount
FROM (
    SELECT id, voltage_filled_max_2_prior, kwh_share_of_site_id, voltage_filled
    FROM with_activity
) src
UNPIVOT (
    amount FOR measure IN (voltage_filled_max_2_prior, kwh_share_of_site_id, voltage_filled)
) unp
ORDER BY id, measure
