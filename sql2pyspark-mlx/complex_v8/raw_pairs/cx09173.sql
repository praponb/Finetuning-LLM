WITH scoped_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_to,
        rate
    FROM tariff_windows
    WHERE tariff <> 'TOU'
)
SELECT id AS qty
FROM scoped_tariffs
WHERE tariff IN ('FLAT', 'EV')
  AND rate BETWEEN 3.0 AND 7.8
INTERSECT
SELECT id AS qty
FROM sites
WHERE commissioned_on BETWEEN DATE '2024-08-24' AND DATE '2025-11-28'
ORDER BY qty
