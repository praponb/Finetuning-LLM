WITH filtered_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        CASE WHEN rate >= 6.1 THEN 'large' WHEN rate >= 4.2 THEN 'medium' ELSE 'small' END AS rate_tier,
        DATEDIFF(DATE '2026-01-01', valid_from) AS days_since_valid_from,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(valid_to)) / 60, 1) AS minutes_valid_to_to_valid_from
    FROM tariff_windows
    WHERE valid_to < TIMESTAMP '2025-03-27 13:00:00'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        valid_from,
        rate,
        minutes_valid_to_to_valid_from,
        COUNT(*) OVER (PARTITION BY YEAR(valid_to), rate_tier) AS rows_in_valid_to_year_rate_tier,
        RANK() OVER w_valid_to_month_by_rate AS rate_rank,
        ROUND(days_since_valid_from / NULLIF(SUM(days_since_valid_from) OVER w_valid_to_year_rate_tier, 0), 4) AS days_since_valid_from_share_of_valid_to_year_rat
    FROM filtered_tariffs
    WINDOW w_valid_to_month_by_rate AS (PARTITION BY MONTH(valid_to) ORDER BY rate, id), w_valid_to_year_rate_tier AS (PARTITION BY YEAR(valid_to), rate_tier)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        valid_from,
        rate,
        days_since_valid_from_share_of_valid_to_year_rat,
        ABS(rate_rank - 7) AS rate_rank_distance,
        rate_rank - rate AS rate_rank_minus_rate
    FROM sequenced
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE rate_rank_distance > (SELECT AVG(rate_rank_distance) FROM tagged)
)
SELECT id, metric, reading
FROM (
    SELECT id, days_since_valid_from_share_of_valid_to_year_rat, rate
    FROM qualified
) src
UNPIVOT (
    reading FOR metric IN (days_since_valid_from_share_of_valid_to_year_rat, rate)
) unp
ORDER BY id, metric
