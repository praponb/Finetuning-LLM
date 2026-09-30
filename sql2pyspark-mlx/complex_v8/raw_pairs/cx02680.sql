WITH valid_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        CASE WHEN tariff IN ('FLAT', 'EV') THEN 'group_a' WHEN tariff = 'TOU' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM tariff_windows
    WHERE valid_to >= TIMESTAMP '2025-02-15 04:00:00'
)
SELECT id, rate
FROM tariff_windows
WHERE tariff = 'EV'
UNION
SELECT id, installed_kw
FROM sites
ORDER BY rate
