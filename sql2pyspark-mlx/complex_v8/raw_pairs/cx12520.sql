WITH base_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        zone,
        tariff,
        commissioned_on
    FROM sites
    WHERE zone <> 'Z3'
      AND tariff IS NOT NULL
)
SELECT id
FROM base_sites
WHERE commissioned_on < DATE '2025-05-10'
EXCEPT
SELECT id
FROM meter_readings
ORDER BY id DESC
