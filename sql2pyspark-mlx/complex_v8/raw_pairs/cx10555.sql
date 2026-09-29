WITH cleaned_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug,
        DATE_FORMAT(valid_from, 'yyyy-MM') AS valid_from_label
    FROM tariff_windows
    WHERE valid_to < TIMESTAMP '2025-03-09 05:00:00'
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM cleaned_tariffs
    WHERE rate < (SELECT MAX(rate) FROM cleaned_tariffs)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY MONTH(valid_from) ORDER BY rate DESC, id) AS rn,
        ROUND(AVG(rate) OVER (PARTITION BY MONTH(valid_from) ORDER BY rate DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rate_avg_7,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from)) AS rows_in_valid_from_month,
        MIN(rate) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from)) AS valid_from_month_min_rate,
        MIN(rate) OVER (PARTITION BY MONTH(valid_from) ORDER BY rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_rate_so_far
    FROM with_activity
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM ranked
    WHERE rn > (SELECT MIN(rn) FROM ranked)
)
-- Final projection
SELECT
    id,
    valid_from,
    rate,
    tariff_slug,
    rate_avg_7,
    rows_in_valid_from_month,
    valid_from_month_min_rate,
    DATEDIFF(DATE '2025-12-31', valid_to) AS days_since_valid_to,
    CASE WHEN rows_in_valid_from_month >= 17 THEN 'A' WHEN rows_in_valid_from_month >= 10 THEN 'B' ELSE 'C' END AS rows_in_valid_from_month_tier
FROM qualified
ORDER BY rate_avg_7 DESC NULLS FIRST, days_since_valid_to DESC
