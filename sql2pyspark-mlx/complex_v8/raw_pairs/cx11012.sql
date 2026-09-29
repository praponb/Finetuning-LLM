SELECT
    id AS qty,
    rate AS value,
    tariff,
    'tariff' AS src
FROM tariff_windows
WHERE tariff NOT IN ('TOU', 'EV')
UNION ALL
SELECT
    id AS qty,
    rate AS value,
    tariff,
    'tariff_2' AS src
FROM tariff_windows
