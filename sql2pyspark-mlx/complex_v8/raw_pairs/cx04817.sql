WITH valid_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        DATE_FORMAT(valid_from, 'yyyy-MM') AS valid_from_label
    FROM tariff_windows
    WHERE tariff IS NOT NULL
)
SELECT 'tariff' AS src, id AS key_id, valid_from_label
FROM valid_tariffs
WHERE valid_from_label IS NOT NULL
  AND valid_from >= TIMESTAMP '2025-01-24 14:00:00'
UNION
SELECT 'site' AS src, id AS key_id, zone AS valid_from_label
FROM sites
WHERE installed_kw BETWEEN 66.5 AND 386.6
ORDER BY src DESC
