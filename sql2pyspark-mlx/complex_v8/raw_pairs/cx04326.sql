WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 9
)
SELECT site_id
FROM valid
EXCEPT
SELECT id
FROM sites
WHERE UPPER(TRIM(site_name)) LIKE 'D%'
  AND commissioned_on >= DATE '2024-09-15'
ORDER BY site_id DESC
