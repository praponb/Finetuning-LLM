WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        CASE WHEN rate >= 5.9 THEN 'large' WHEN rate >= 3.7 THEN 'medium' ELSE 'small' END AS rate_tier
    FROM tariff_windows
    WHERE rate < 5.5
      AND valid_from < TIMESTAMP '2025-02-16 11:00:00'
),
latest_per_key AS (
    -- Keep the latest row per tariff, rate_tier
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        tariff_or_unknown,
        rate_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff, rate_tier ORDER BY rate DESC, id DESC) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown_2,
        YEAR(valid_to) AS valid_to_year
    FROM latest_per_key
    WHERE tariff_or_unknown IS NOT NULL
),
rate_tier_stats AS (
    -- Avg rate per rate_tier
    SELECT rate_tier, AVG(rate) AS avg_rate
    FROM scored
    GROUP BY rate_tier
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rate, ROUND(s.rate - g.avg_rate, 2) AS rate_minus_avg
    FROM scored s
    JOIN rate_tier_stats g ON g.rate_tier = s.rate_tier
    WHERE s.rate > g.avg_rate
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from), tariff_or_unknown_2) AS rows_in_valid_from_month_tariff_or_unknown_2,
        SUM(avg_rate) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_to) ORDER BY rate_minus_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_rate,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', valid_to) ORDER BY rate_minus_avg ASC, id) AS rate_minus_avg_seq,
        ROUND(avg_rate - AVG(avg_rate) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from), tariff_or_unknown_2), 2) AS avg_rate_vs_valid_from_month_tariff_or_unknown_2
    FROM outliers
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(rate_tier) OVER (PARTITION BY valid_to_year)) AS distinct_rate_tier_in_valid_to_year,
        SIZE(COLLECT_SET(tariff_or_unknown) OVER w_valid_to_year) AS distinct_tariff_or_unknown_in_valid_to_year,
        rate - LAG(rate) OVER (PARTITION BY rate_minus_avg_seq, valid_to_year ORDER BY running_avg_rate DESC, id) AS rate_change
    FROM with_windows
    WINDOW w_valid_to_year AS (PARTITION BY valid_to_year)
)
-- Final projection
SELECT
    id,
    tariff,
    valid_from,
    valid_to,
    rate,
    tariff_or_unknown,
    rate_tier,
    tariff_or_unknown_2,
    valid_to_year,
    avg_rate,
    rate_minus_avg,
    rows_in_valid_from_month_tariff_or_unknown_2,
    avg_rate_vs_valid_from_month_tariff_or_unknown_2,
    distinct_tariff_or_unknown_in_valid_to_year,
    rate_change,
    LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug,
    LOWER(TRIM(rate_tier)) AS rate_tier_clean
FROM sequenced
ORDER BY rate DESC NULLS FIRST, rate_minus_avg DESC, id
LIMIT 20
