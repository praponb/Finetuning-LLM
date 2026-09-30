WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        ABS(voltage - 223.1) AS voltage_distance
    FROM meter_readings
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM scoped_readings
    WHERE kwh < 80.8
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(voltage_distance - 73.5) AS voltage_distance_distance,
        LENGTH(TRIM(quality_group)) AS quality_group_len
    FROM scored
)
SELECT *
FROM (
    SELECT site_id, quality, voltage_distance
    FROM annotated
) src
PIVOT (
    COUNT(voltage_distance)
    FOR quality IN ('BAD', 'OK')
)
ORDER BY site_id
