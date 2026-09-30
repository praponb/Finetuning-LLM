WITH filtered_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE tariff IN ('EV', 'TOU')
      AND rate < 5.5
)
SELECT id
FROM filtered_tariffs
EXCEPT
SELECT id
FROM sites
WHERE installed_kw > 141.6
ORDER BY id
