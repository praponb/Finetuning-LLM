WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 12
      AND site_id <= 3
)
SELECT id AS entity_id, quality_clean
FROM recent
INTERSECT
SELECT id AS entity_id, tariff AS quality_clean
FROM tariff_windows
WHERE valid_from < TIMESTAMP '2025-02-18 04:00:00'
