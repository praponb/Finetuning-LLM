WITH cleaned AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        DATEDIFF(DATE '2026-01-01', valid_from) AS days_since_valid_from
    FROM tariff_windows
    WHERE tariff IS NOT NULL
      AND TO_DATE(valid_to) < DATE '2025-03-12'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tariff,
        LEAD(rate, 1, 0) OVER (PARTITION BY MONTH(valid_from) ORDER BY days_since_valid_from DESC, id) AS next_rate,
        NTILE(10) OVER (PARTITION BY MONTH(valid_from) ORDER BY valid_from, id) AS valid_from_decile,
        ROUND(AVG(days_since_valid_from) OVER (PARTITION BY MONTH(valid_from) ORDER BY days_since_valid_from DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS days_since_valid_from_avg_3_prior,
        LAST_VALUE(valid_from) IGNORE NULLS OVER (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_from) ORDER BY days_since_valid_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_valid_from,
        LEAD(rate, 2) OVER (PARTITION BY MONTH(valid_from) ORDER BY valid_from, id) AS next2_rate,
        MIN(days_since_valid_from) OVER (PARTITION BY tariff) AS tariff_min_days_since_valid_from,
        rate - LAG(rate) OVER w_tariff_valid_from_month_by_d_2 AS rate_change,
        NTH_VALUE(days_since_valid_from, 2) OVER (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_from) ORDER BY days_since_valid_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_valid_from,
        LEAD(valid_from) OVER (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_from) ORDER BY days_since_valid_from, id) AS next_valid_from
    FROM cleaned
    WINDOW w_tariff_valid_from_month_by_d_2 AS (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_from) ORDER BY days_since_valid_from, id)
)
-- Final projection
SELECT
    id,
    tariff,
    next_rate,
    valid_from_decile,
    days_since_valid_from_avg_3_prior,
    last_known_valid_from,
    next2_rate,
    rate_change,
    second_days_since_valid_from,
    next_valid_from,
    COALESCE(valid_from_decile, 0) AS valid_from_decile_filled
FROM with_trends
WHERE TO_DATE(last_known_valid_from) < DATE '2025-03-09'
ORDER BY second_days_since_valid_from DESC NULLS FIRST, days_since_valid_from_avg_3_prior, id
LIMIT 5
