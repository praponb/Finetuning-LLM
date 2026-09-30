WITH scoped_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        DATEDIFF(DATE '2025-06-30', valid_to) AS days_since_valid_to
    FROM tariff_windows
    WHERE (tariff = 'FLAT' OR TO_DATE(valid_from) < DATE '2025-03-09')
),
latest AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        days_since_valid_to
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY days_since_valid_to DESC, id DESC) AS rn
        FROM scoped_tariffs
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        tariff,
        rate,
        days_since_valid_to,
        ABS(rate - 5.2) AS rate_distance
    FROM latest
    WHERE rate >= 4.2
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        tariff,
        rate,
        days_since_valid_to,
        rate_distance,
        COALESCE(days_since_valid_to, 0) AS days_since_valid_to_filled,
        ABS(days_since_valid_to - 385) AS days_since_valid_to_distance,
        LOWER(TRIM(tariff)) AS tariff_clean
    FROM enriched
    WHERE days_since_valid_to >= 280
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tariff,
        days_since_valid_to_filled,
        days_since_valid_to_distance,
        ROUND(CUME_DIST() OVER (PARTITION BY tariff ORDER BY days_since_valid_to_filled DESC NULLS FIRST, id), 3) AS days_since_valid_to_filled_cume_dist,
        ROUND(rate - AVG(rate) OVER w_tariff, 2) AS rate_vs_tariff_avg,
        LAST_VALUE(days_since_valid_to) OVER (PARTITION BY tariff ORDER BY days_since_valid_to_filled DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_valid_to
    FROM scored
    WINDOW w_tariff AS (PARTITION BY tariff)
)
-- Final projection
SELECT
    id,
    tariff,
    days_since_valid_to_filled,
    days_since_valid_to_filled_cume_dist,
    rate_vs_tariff_avg,
    last_days_since_valid_to
FROM ranked
ORDER BY id, days_since_valid_to_filled_cume_dist
LIMIT 5
