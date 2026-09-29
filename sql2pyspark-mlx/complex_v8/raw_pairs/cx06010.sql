WITH scoped AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        LOWER(TRIM(tariff)) AS tariff_clean
    FROM tariff_windows
    WHERE (tariff IS NOT NULL OR MONTH(valid_to) IN (1, 3))
)
SELECT tariff, valid_from
FROM scoped
WHERE rate < 5.5
  AND valid_from < TIMESTAMP '2025-02-16 11:00:00'
UNION
SELECT tariff, valid_from
FROM tariff_windows
ORDER BY valid_from DESC, tariff
