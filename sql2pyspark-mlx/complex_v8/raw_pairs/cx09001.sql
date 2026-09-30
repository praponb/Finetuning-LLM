WITH prepared_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE (tariff IS NOT NULL OR valid_to >= TIMESTAMP '2025-01-25 21:00:00')
)
SELECT id, tariff AS name
FROM prepared_tariffs
EXCEPT
SELECT id, zone AS name
FROM sites
WHERE tariff <> 'TOU'
  AND commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-05-10'
ORDER BY name
