WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        DATE_TRUNC('MONTH', valid_to) AS valid_to_month,
        DATE_FORMAT(valid_from, 'yyyy-MM-dd HH:00') AS valid_from_label
    FROM tariff_windows
    WHERE tariff IN ('EV', 'TOU')
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM recent
    WHERE rate < (SELECT MAX(rate) FROM recent)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(rate) OVER (PARTITION BY tariff, MONTH(valid_from) ORDER BY valid_to_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_rate_so_far,
        NTH_VALUE(valid_from, 2) OVER (PARTITION BY tariff, MONTH(valid_from) ORDER BY valid_to_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_valid_from,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY MONTH(valid_from)), 0), 4) AS rate_share_of_valid_from_month,
        FIRST_VALUE(rate) IGNORE NULLS OVER (ORDER BY rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rate,
        SUM(rate) OVER (ORDER BY rate DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS rate_sum_1_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tariff, TO_DATE(valid_to) ORDER BY rate DESC), 3) AS rate_percent_rank
    FROM qualified
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        valid_to_month,
        min_rate_so_far,
        second_valid_from,
        first_known_rate,
        rate_sum_1_prior,
        rate_percent_rank,
        ROUND(rate * 0.9, 2) AS rate_adj
    FROM with_windows
    WHERE valid_from >= TIMESTAMP '2025-01-24 14:00:00'
)
SELECT
    id,
    valid_from,
    rate,
    valid_to_month,
    min_rate_so_far,
    rate_percent_rank,
    DATE_TRUNC('WEEK', valid_to_month) AS valid_to_month_week,
    SUBSTRING(TRIM(tariff), 1, 3) AS tariff_prefix
FROM enriched
ORDER BY tariff_prefix NULLS LAST
