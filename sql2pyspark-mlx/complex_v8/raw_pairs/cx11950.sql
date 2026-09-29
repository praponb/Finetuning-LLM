WITH cleaned_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
),
latest_per_key AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY valid_from DESC, id DESC) AS rn
        FROM cleaned_tariffs
    ) ranked
    WHERE rn = 1
),
latest_per_key_2 AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY rate DESC, id DESC) AS rn
        FROM latest_per_key
    ) ranked
    WHERE rn = 1
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        CASE WHEN rate >= 6.1 THEN 'HIGH' WHEN rate >= 3.7 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier,
        DATEDIFF(DATE '2025-12-31', valid_to) AS days_since_valid_to
    FROM latest_per_key_2
    WHERE valid_to < TIMESTAMP '2025-03-09 05:00:00'
),
annotated_2 AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        DATE_TRUNC('HOUR', valid_to) AS valid_to_hour
    FROM annotated
    WHERE tariff_ref IS NOT NULL
),
latest AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        tariff_ref,
        rate_tier,
        days_since_valid_to,
        tariff_or_unknown,
        valid_to_hour
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY valid_to_hour DESC, id DESC) AS row_num
        FROM annotated_2
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(rate_tier, 'ALL') AS rate_tier,
    COALESCE(tariff, 'ALL') AS tariff,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(rate), 2) AS stddev_rate,
    ROUND(SUM(rate), 2) AS total_rate,
    COUNT(*) AS cnt
FROM latest
GROUP BY GROUPING SETS ((rate_tier, tariff), (rate_tier), (tariff), ())
ORDER BY rate_tier NULLS LAST, tariff NULLS LAST
