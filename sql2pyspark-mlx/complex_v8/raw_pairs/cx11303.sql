WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tariff_windows
    WHERE (tariff IS NOT NULL OR rate > 3.0)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(LAG(valid_from) OVER w_valid_from_month_by_valid_fr)) / 60, 1) AS minutes_since_prev_valid_from,
        LAST_VALUE(valid_to) OVER (PARTITION BY MONTH(valid_from) ORDER BY valid_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_valid_to,
        ROUND(CUME_DIST() OVER w_valid_from_month_by_valid_fr_3, 3) AS valid_from_cume_dist,
        LAST_VALUE(rate) IGNORE NULLS OVER (PARTITION BY MONTH(valid_from) ORDER BY valid_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rate,
        ROUND(rate - AVG(rate) OVER (PARTITION BY tariff), 2) AS rate_vs_tariff_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(valid_from) ORDER BY valid_from), 3) AS valid_from_percent_rank
    FROM scoped
    WINDOW w_valid_from_month_by_valid_fr AS (PARTITION BY MONTH(valid_from) ORDER BY valid_from, id), w_valid_from_month_by_valid_fr_3 AS (PARTITION BY MONTH(valid_from) ORDER BY valid_from)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        valid_from,
        minutes_since_prev_valid_from,
        last_known_rate,
        valid_from_percent_rank,
        ROUND(rate - AVG(rate) OVER (PARTITION BY MONTH(valid_from), YEAR(valid_to)), 2) AS rate_vs_valid_from_month_valid_to_year_avg,
        ROUND(AVG(valid_from_percent_rank) OVER (ORDER BY minutes_since_prev_valid_from, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS valid_from_percent_rank_avg_4,
        LAST_VALUE(valid_from) OVER (ORDER BY valid_from_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_valid_from,
        ROUND(PERCENT_RANK() OVER (ORDER BY minutes_since_prev_valid_from, id), 3) AS minutes_since_prev_valid_from_percent_rank,
        COUNT(*) OVER (PARTITION BY MONTH(valid_from), YEAR(valid_to)) AS rows_in_valid_from_month_valid_to_year
    FROM with_windows
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, valid_from_percent_rank_avg_4, minutes_since_prev_valid_from_percent_rank
    FROM with_trends
) src
UNPIVOT INCLUDE NULLS (
    amount FOR measure IN (valid_from_percent_rank_avg_4 AS m_valid_from_percent_rank_avg_4, minutes_since_prev_valid_from_percent_rank AS m_minutes_since_prev_valid_from_percent_rank)
) unp
