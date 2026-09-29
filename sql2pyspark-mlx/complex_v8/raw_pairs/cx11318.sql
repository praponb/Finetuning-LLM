WITH valid_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM tariff_windows
    WHERE (valid_from >= TIMESTAMP '2025-02-15 04:00:00' OR tariff NOT IN ('TOU', 'EV'))
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tariff ORDER BY rate DESC), 3) AS rate_percent_rank,
        LAG(valid_from, 3) OVER (PARTITION BY tariff ORDER BY rate DESC, id) AS prev3_valid_from,
        ROUND(rate - AVG(rate) OVER w_tariff, 2) AS rate_vs_tariff_avg,
        NTILE(10) OVER (PARTITION BY tariff ORDER BY rate DESC, id) AS rate_decile,
        RANK() OVER w_tariff_by_rate_ties AS rate_rank
    FROM valid_tariffs
    WINDOW w_tariff AS (PARTITION BY tariff), w_tariff_by_rate_ties AS (PARTITION BY tariff ORDER BY rate DESC)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        rate_percent_rank,
        prev3_valid_from,
        rate_vs_tariff_avg,
        rate_decile,
        rate_rank,
        ROUND(rate_rank / NULLIF(SUM(rate_rank) OVER (PARTITION BY rate_decile), 0), 4) AS rate_rank_share_of_rate_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY rate_rank ORDER BY rate_rank DESC), 3) AS rate_rank_percent_rank
    FROM ranked
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        rate_percent_rank,
        prev3_valid_from,
        rate_vs_tariff_avg,
        rate_rank,
        LAST_VALUE(rate_percent_rank) OVER (PARTITION BY tariff, rate_rank ORDER BY rate_percent_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rate_percent_rank,
        LAST_VALUE(rate_vs_tariff_avg) OVER (PARTITION BY MONTH(valid_from) ORDER BY rate_decile ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rate_vs_tariff_avg
    FROM with_windows
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        tariff,
        rate,
        rate_percent_rank,
        prev3_valid_from,
        rate_vs_tariff_avg,
        rate_rank,
        GREATEST(rate_vs_tariff_avg, last_rate_percent_rank) AS greatest_rate_vs_tariff_avg_last_rate_percent_ra
    FROM windowed
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(rate_percent_rank, rate_rank) AS greatest_rate_percent_rank_rate_rank,
        DATEDIFF(DATE '2025-06-30', prev3_valid_from) AS days_since_prev3_valid_from
    FROM enriched
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tariff,
        prev3_valid_from,
        rate_rank,
        greatest_rate_vs_tariff_avg_last_rate_percent_ra,
        days_since_prev3_valid_from,
        MAX(rate_percent_rank) OVER (PARTITION BY tariff, rate_rank ORDER BY prev3_valid_from, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rate_percent_rank_max_4,
        ROUND(rate_percent_rank / NULLIF(SUM(rate_percent_rank) OVER w_tariff, 0), 4) AS rate_percent_rank_share_of_tariff
    FROM tagged
    WINDOW w_tariff AS (PARTITION BY tariff)
)
-- Final projection
SELECT
    id,
    tariff,
    prev3_valid_from,
    rate_rank,
    greatest_rate_vs_tariff_avg_last_rate_percent_ra,
    rate_percent_rank_max_4,
    rate_percent_rank_share_of_tariff,
    rate_rank + rate_percent_rank_max_4 AS rate_rank_plus_rate_percent_rank_max_4
FROM sequenced
WHERE rate_rank <= 6
ORDER BY rate_percent_rank_max_4
