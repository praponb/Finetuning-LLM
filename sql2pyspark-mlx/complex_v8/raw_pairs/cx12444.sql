WITH cleaned AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE TO_DATE(valid_to) < DATE '2025-03-12'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY YEAR(valid_to) ORDER BY rate DESC, id) AS rate_rank,
        NTH_VALUE(rate, 2) OVER (ORDER BY valid_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rate,
        FIRST_VALUE(valid_from) IGNORE NULLS OVER (PARTITION BY YEAR(valid_to) ORDER BY rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_valid_from,
        MAX(rate) OVER w_valid_to_month AS valid_to_month_max_rate
    FROM cleaned
    WINDOW w_valid_to_month AS (PARTITION BY MONTH(valid_to))
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked
    WHERE second_rate > (SELECT AVG(second_rate) FROM ranked)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        valid_from,
        rate,
        ROUND(rate_rank / NULLIF(SUM(rate_rank) OVER (PARTITION BY rate_rank), 0), 4) AS rate_rank_share_of_rate_rank,
        ROUND(second_rate - AVG(second_rate) OVER (PARTITION BY rate_rank), 2) AS second_rate_vs_rate_rank_avg,
        RANK() OVER (ORDER BY valid_from DESC) AS valid_from_rank
    FROM with_activity
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(rate, 0.0) AS rate_filled,
        DATE_TRUNC('YEAR', valid_from) AS valid_from_year,
        valid_from + INTERVAL 30 MINUTES AS valid_from_plus_30m
    FROM metrics
    WHERE rate > 3.0
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(second_rate_vs_rate_rank_avg, 2) OVER (PARTITION BY valid_from_rank ORDER BY valid_from DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_rate_vs_rate_rank_avg,
        NTH_VALUE(valid_from_rank, 2) OVER (PARTITION BY valid_from_rank ORDER BY valid_from DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_valid_from_rank,
        MAX(second_rate_vs_rate_rank_avg) OVER (ORDER BY valid_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_second_rate_vs_rate_rank_avg_so_far,
        ROUND(CUME_DIST() OVER (ORDER BY valid_from, id), 3) AS valid_from_cume_dist,
        ROUND(AVG(rate) OVER w_valid_from_rank_valid_from_p, 2) AS valid_from_rank_valid_from_plus_30m_year_avg_rat
    FROM tagged
    WINDOW w_valid_from_rank_valid_from_p AS (PARTITION BY valid_from_rank, YEAR(valid_from_plus_30m))
)
-- Final projection
SELECT
    id,
    rate,
    second_rate_vs_rate_rank_avg,
    valid_from_rank,
    rate_filled,
    valid_from_year,
    valid_from_plus_30m,
    second_second_rate_vs_rate_rank_avg,
    second_valid_from_rank,
    valid_from_rank_valid_from_plus_30m_year_avg_rat
FROM with_windows
ORDER BY second_second_rate_vs_rate_rank_avg
