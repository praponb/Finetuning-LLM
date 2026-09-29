WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage BETWEEN 221.4 AND 241.8
      AND MONTH(read_at) IN (2, 3)
)
SELECT site_id
FROM base_readings
WHERE kwh <= 81.3
  AND site_id >= 7
EXCEPT
SELECT id
FROM sites
WHERE tariff = 'FLAT'
  AND installed_kw <= 284.0
