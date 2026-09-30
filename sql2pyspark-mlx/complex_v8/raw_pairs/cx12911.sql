WITH recent_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        DATEDIFF(DATE '2025-06-30', valid_to) AS days_since_valid_to,
        SUBSTRING(TRIM(tariff), 1, 4) AS tariff_prefix,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref
    FROM tariff_windows
    WHERE HOUR(valid_to) BETWEEN 10 AND 14
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        valid_to,
        rate,
        tariff_prefix,
        tariff_ref,
        days_since_valid_to + rate AS days_since_valid_to_plus_rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM recent_tariffs
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, rate, days_since_valid_to_plus_rate
    FROM enriched
) src
UNPIVOT (
    metric_value FOR measure IN (rate AS rate_v, days_since_valid_to_plus_rate AS days_since_valid_to_plus_rate_v)
) unp
