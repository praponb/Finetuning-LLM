WITH scoped_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug,
        DATEDIFF(DATE '2025-12-31', valid_to) AS days_since_valid_to,
        SUBSTRING(TRIM(tariff), 1, 3) AS tariff_prefix
    FROM tariff_windows
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        tariff_slug,
        tariff_prefix,
        NTILE(3) OVER (ORDER BY rate, id) AS rate_tercile,
        LEAD(rate, 2) OVER w_all_by_rate AS next2_rate
    FROM scoped_tariffs
    WINDOW w_all_by_rate AS (ORDER BY rate, id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        tariff_slug,
        tariff_prefix,
        rate_tercile,
        next2_rate,
        ROUND(CUME_DIST() OVER w_valid_to_month_by_next2_rate, 3) AS next2_rate_cume_dist,
        ROUND(AVG(rate_tercile) OVER (PARTITION BY rate_tercile, tariff), 2) AS rate_tercile_tariff_avg_rate_tercile
    FROM with_windows
    WINDOW w_valid_to_month_by_next2_rate AS (PARTITION BY MONTH(valid_to) ORDER BY next2_rate NULLS LAST)
),
deduped AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        tariff_slug,
        tariff_prefix,
        rate_tercile,
        next2_rate,
        next2_rate_cume_dist,
        rate_tercile_tariff_avg_rate_tercile
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY valid_to DESC, id DESC) AS row_num
        FROM sequenced
    ) ranked
    WHERE row_num = 1
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, rate_tercile_tariff_avg_rate_tercile, next2_rate
    FROM deduped
) src
UNPIVOT (
    metric_value FOR metric_name IN (rate_tercile_tariff_avg_rate_tercile, next2_rate)
) unp
ORDER BY id, metric_name
