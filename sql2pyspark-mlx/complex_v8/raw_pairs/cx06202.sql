WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-02 11:00:00'
)
SELECT quality AS name
FROM prepared
EXCEPT
SELECT tariff
FROM sites
WHERE installed_kw BETWEEN 66.5 AND 284.0
  AND commissioned_on >= DATE '2024-10-28'
ORDER BY name DESC
