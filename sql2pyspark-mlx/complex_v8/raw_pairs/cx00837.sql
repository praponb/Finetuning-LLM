WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE (HOUR(read_at) BETWEEN 6 AND 10 OR kwh < 109.0)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        voltage,
        quality,
        quality_len,
        ROUND(quality_len / NULLIF(voltage, 0), 3) AS quality_len_per_voltage
    FROM scoped_readings
)
SELECT
    quality,
    quality_len,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM scored
GROUP BY CUBE(quality, quality_len)
