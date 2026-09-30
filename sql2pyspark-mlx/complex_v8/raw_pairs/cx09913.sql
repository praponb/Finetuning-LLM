WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        DATEDIFF(DATE '2026-01-01', valid_to) AS days_since_valid_to,
        DATEDIFF(DATE '2026-01-01', valid_from) AS days_since_valid_from
    FROM tariff_windows
    WHERE valid_to >= TIMESTAMP '2025-02-15 04:00:00'
      AND MONTH(valid_from) IN (1, 3)
),
tariff_stats AS (
    -- Max days_since_valid_to per tariff
    SELECT tariff, MAX(days_since_valid_to) AS max_days_since_valid_to
    FROM base
    GROUP BY tariff
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_days_since_valid_to,
        ROUND(s.days_since_valid_to - g.max_days_since_valid_to, 2) AS days_since_valid_to_minus_max
    FROM base s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.days_since_valid_to = g.max_days_since_valid_to
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(days_since_valid_to) OVER (PARTITION BY tariff ORDER BY valid_to, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS days_since_valid_to_avg_3_prior,
        LAST_VALUE(max_days_since_valid_to) OVER (PARTITION BY tariff ORDER BY valid_to, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_max_days_since_valid_to,
        LAG(valid_from) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_to) ORDER BY days_since_valid_to ASC, id) AS prev_valid_from,
        COUNT(*) OVER (PARTITION BY tariff) AS rows_in_tariff,
        ROUND(days_since_valid_from / NULLIF(SUM(days_since_valid_from) OVER w_tariff, 0), 4) AS days_since_valid_from_share_of_tariff
    FROM outliers
    WINDOW w_tariff AS (PARTITION BY tariff)
)
-- Final projection
SELECT
    id,
    tariff,
    valid_from,
    valid_to,
    days_since_valid_to,
    max_days_since_valid_to,
    days_since_valid_to_minus_max,
    last_max_days_since_valid_to,
    prev_valid_from,
    rows_in_tariff
FROM windowed
WHERE days_since_valid_to_minus_max < 420.0
ORDER BY prev_valid_from DESC
