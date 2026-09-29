WITH cleaned_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        ABS(rate - 5.5) AS rate_distance
    FROM tariff_windows
    WHERE rate <= 5.9
      AND valid_to < TIMESTAMP '2025-03-09 04:00:00'
)
SELECT id
FROM cleaned_tariffs
WHERE tariff IS NOT NULL
  AND rate_distance >= 2.0
EXCEPT ALL
SELECT id
FROM tariff_windows
