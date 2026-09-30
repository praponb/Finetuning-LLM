WITH prepared AS (
    -- Filter tariff_windows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        ROUND(rate * 0.9, 2) AS rate_adj,
        MONTH(valid_to) AS valid_to_month
    FROM tariff_windows
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared
    WHERE rate > (SELECT AVG(rate) FROM prepared)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        rate_adj,
        valid_to_month,
        rate_adj - LAG(rate_adj) OVER w_valid_to_month_by_rate_adj AS rate_adj_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY rate), 3) AS rate_percent_rank,
        MAX(rate) OVER (PARTITION BY valid_to_month) AS valid_to_month_max_rate,
        SUM(rate_adj) OVER (PARTITION BY valid_to_month) AS valid_to_month_sum_rate_adj,
        FIRST_VALUE(valid_to_month) IGNORE NULLS OVER (ORDER BY rate, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_valid_to_month
    FROM filtered
    WINDOW w_valid_to_month_by_rate_adj AS (PARTITION BY valid_to_month ORDER BY rate_adj, id)
)
SELECT
    id,
    valid_from,
    valid_to,
    rate,
    rate_adj,
    valid_to_month,
    rate_adj_change,
    rate_percent_rank,
    valid_to_month_max_rate,
    first_known_valid_to_month,
    FLOOR(valid_to_month_max_rate / 10) * 10 AS valid_to_month_max_rate_bucket
FROM metrics
ORDER BY rate_adj_change DESC, first_known_valid_to_month NULLS LAST
