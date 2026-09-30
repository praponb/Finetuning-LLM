WITH prepared AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        valid_to + INTERVAL 7 DAYS AS valid_to_plus_7d
    FROM tariff_windows
    WHERE tariff = 'EV'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        tariff,
        rate,
        valid_to_plus_7d,
        RANK() OVER w_valid_to_plus_7d_month_tarif AS valid_from_rank,
        ROUND(AVG(rate) OVER (PARTITION BY tariff), 2) AS tariff_avg_rate,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(LAG(valid_from) OVER w_valid_to_plus_7d_month_tarif)) / 60, 1) AS minutes_since_prev_valid_from,
        MAX(rate) OVER (PARTITION BY tariff) AS tariff_max_rate
    FROM prepared
    WINDOW w_valid_to_plus_7d_month_tarif AS (PARTITION BY MONTH(valid_to_plus_7d), tariff ORDER BY valid_from DESC, id)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE tariff_max_rate < (SELECT MAX(tariff_max_rate) FROM sequenced)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        rate,
        valid_from_rank,
        tariff_max_rate,
        ROUND(tariff_avg_rate * 0.9, 2) AS tariff_avg_rate_adj
    FROM qualified
    WHERE tariff_avg_rate > 4.4
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE tariff_avg_rate_adj > (SELECT AVG(tariff_avg_rate_adj) FROM tagged)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY tariff_max_rate, id) AS tariff_max_rate_seq,
        COUNT(*) OVER w_valid_from_rank AS rows_in_valid_from_rank,
        LAST_VALUE(tariff_avg_rate_adj) IGNORE NULLS OVER (ORDER BY tariff_max_rate, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_tariff_avg_rate_adj
    FROM screened
    WINDOW w_valid_from_rank AS (PARTITION BY valid_from_rank)
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, tariff_max_rate_seq, valid_from_rank, rows_in_valid_from_rank
    FROM windowed
) src
UNPIVOT (
    metric_value FOR metric_name IN (tariff_max_rate_seq, valid_from_rank, rows_in_valid_from_rank)
) unp
ORDER BY id, metric_name
