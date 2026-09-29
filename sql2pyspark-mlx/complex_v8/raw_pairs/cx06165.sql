SELECT
    tariff,
    id,
    rate AS value,
    'tariff' AS source
FROM tariff_windows
UNION ALL
SELECT
    tariff,
    id,
    rate,
    'tariff_2' AS source
FROM tariff_windows
ORDER BY source DESC
