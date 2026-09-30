WITH base_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to
    FROM tariff_windows
    WHERE HOUR(valid_to) BETWEEN 8 AND 11
      AND rate <= 7.6
)
SELECT id, tariff AS category
FROM base_tariffs
WHERE valid_from < TIMESTAMP '2025-02-18 04:00:00'
INTERSECT ALL
SELECT id, tariff AS category
FROM tariff_windows
WHERE rate > 4.2
  AND tariff NOT IN ('FLAT', 'EV')
ORDER BY id, category
