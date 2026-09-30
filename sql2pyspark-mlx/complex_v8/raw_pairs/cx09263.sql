WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_to,
        rate
    FROM tariff_windows
    WHERE TO_DATE(valid_to) < DATE '2025-04-05'
)
SELECT tariff
FROM filtered
WHERE rate BETWEEN 2.2 AND 5.7
INTERSECT
SELECT quality AS tariff
FROM meter_readings
WHERE kwh <= 79.4
ORDER BY tariff
