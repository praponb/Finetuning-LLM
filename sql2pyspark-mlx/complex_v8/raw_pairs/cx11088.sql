WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality_len,
        NTH_VALUE(voltage, 2) OVER (ORDER BY quality_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        LAST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        LAG(voltage, 2, 0) OVER (PARTITION BY site_id, quality_len ORDER BY read_at, id) AS prev2_voltage
    FROM valid
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 78.6
        )
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    quality_len,
    last_known_kwh,
    prev2_voltage
FROM screened
WHERE kwh <= 85.7
ORDER BY id, kwh DESC
