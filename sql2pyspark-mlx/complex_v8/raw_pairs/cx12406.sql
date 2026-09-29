WITH base AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE HOUR(valid_to) BETWEEN 7 AND 9
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM base
    WHERE rate < (SELECT MAX(rate) FROM base)
)
-- Final projection
SELECT
    id,
    CASE WHEN rate >= 5.7 THEN 'HIGH' WHEN rate >= 4.2 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier,
    DATE_FORMAT(valid_to, 'yyyy-MM-dd HH:00') AS valid_to_label
FROM filtered
WHERE HOUR(valid_from) BETWEEN 6 AND 9
