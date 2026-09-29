WITH prepared AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown_2,
        DATEDIFF(DATE '2026-01-01', valid_from) AS days_since_valid_from
    FROM tariff_windows
    WHERE MONTH(valid_from) IN (1, 3)
      AND HOUR(valid_to) BETWEEN 10 AND 12
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared
    WHERE rate > (SELECT AVG(rate) FROM prepared)
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered
    WHERE rate > (SELECT MIN(rate) FROM filtered)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(valid_from)) / 3600, 1) AS hours_valid_from_to_valid_to,
        CASE WHEN days_since_valid_from >= 420 THEN 'HIGH' WHEN days_since_valid_from >= 175 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_valid_from_tier,
        CONCAT(tariff_or_unknown_2, '#', CAST(id AS STRING)) AS tariff_or_unknown_2_ref
    FROM with_activity
)
SELECT
    id,
    tariff,
    valid_from,
    valid_to,
    tariff_or_unknown,
    tariff_or_unknown_2,
    days_since_valid_from,
    days_since_valid_from_tier,
    tariff_or_unknown_2_ref,
    COALESCE(tariff_or_unknown, 'UNKNOWN') AS tariff_or_unknown_or_unknown,
    ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(valid_to)) / 3600, 1) AS hours_valid_to_to_valid_from
FROM scored
ORDER BY id DESC
