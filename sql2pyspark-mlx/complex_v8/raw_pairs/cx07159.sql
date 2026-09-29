WITH filtered AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(valid_to)) / 3600, 1) AS hours_valid_to_to_valid_from,
        DATEDIFF(DATE '2025-12-31', valid_from) AS days_since_valid_from,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(valid_to)) / 3600, 1) AS hours_valid_to_to_valid_from_2
    FROM tariff_windows
    WHERE rate BETWEEN 2.2 AND 5.9
      AND TO_DATE(valid_to) < DATE '2025-03-12'
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered
    WHERE hours_valid_to_to_valid_from > (SELECT MIN(hours_valid_to_to_valid_from) FROM filtered)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, hours_valid_to_to_valid_from, rate
    FROM screened
) src
UNPIVOT (
    amount FOR metric IN (hours_valid_to_to_valid_from, rate)
) unp
