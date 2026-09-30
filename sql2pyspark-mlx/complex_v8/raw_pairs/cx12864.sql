WITH prepared_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_to,
        rate
    FROM tariff_windows
    WHERE HOUR(valid_from) BETWEEN 7 AND 13
      AND tariff = 'FLAT'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ABS(rate - 5.9) AS rate_distance,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM prepared_tariffs
    WHERE HOUR(valid_to) BETWEEN 8 AND 10
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN rate >= 5.9 THEN 'A' WHEN rate >= 3.0 THEN 'B' ELSE 'C' END AS rate_tier,
        DATEDIFF(DATE '2026-01-01', valid_to) AS days_since_valid_to,
        INITCAP(TRIM(tariff_slug)) AS tariff_slug_clean
    FROM annotated
)
SELECT
    rate_tier,
    tariff,
    MIN(rate_distance) AS min_rate_distance,
    MAX(rate_distance) AS max_rate_distance,
    ROUND(SUM(days_since_valid_to) / NULLIF(SUM(rate), 0), 3) AS days_since_valid_to_to_rate_ratio
FROM labeled
GROUP BY ROLLUP(rate_tier, tariff)
ORDER BY rate_tier NULLS LAST, tariff NULLS LAST
