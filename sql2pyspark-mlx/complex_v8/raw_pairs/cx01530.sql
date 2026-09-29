WITH valid AS (
    -- Scope the sites rows
    SELECT *
    FROM sites
    WHERE commissioned_on < DATE '2025-05-10'
)
SELECT
    'site' AS origin,
    id,
    commissioned_on AS event_date,
    tariff AS label
FROM valid
UNION
SELECT
    'site_2' AS origin,
    id,
    commissioned_on,
    zone
FROM sites
WHERE zone IN ('Z2', 'Z4')
