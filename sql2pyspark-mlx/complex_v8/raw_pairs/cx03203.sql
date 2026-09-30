WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE quality IN ('OK', 'EST')
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_group,
        COALESCE(quality_group, 'UNKNOWN') AS quality_group_or_unknown,
        GREATEST(kwh, voltage) AS greatest_kwh_voltage
    FROM prepared_readings
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM labeled s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE HOUR(m.read_at) BETWEEN 10 AND 13
        )
      AND s.voltage > 220.2
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(quality_group_or_unknown, 'ALL') AS quality_group_or_unknown,
    COALESCE(quality, 'ALL') AS quality,
    COALESCE(quality_group, 'ALL') AS quality_group,
    ROUND(STDDEV(voltage), 2) AS stddev_voltage
FROM qualified
GROUP BY ROLLUP(quality_group_or_unknown, quality, quality_group)
ORDER BY quality_group_or_unknown NULLS LAST, quality NULLS LAST, quality_group NULLS LAST
