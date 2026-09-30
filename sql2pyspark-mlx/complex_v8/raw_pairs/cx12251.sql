WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        ABS(installed_kw - 96.9) AS installed_kw_distance
    FROM sites
    WHERE YEAR(commissioned_on) = 2024
      AND installed_kw < 284.0
)
SELECT id AS qty, installed_kw_distance AS value
FROM prepared
UNION ALL
SELECT id AS qty, kwh AS value
FROM meter_readings
