WITH scoped_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(valid_to)) / 60, 1) AS minutes_valid_to_to_valid_from
    FROM tariff_windows
    WHERE rate >= 3.0
      AND tariff <> 'TOU'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(valid_to, 2) OVER (PARTITION BY tariff ORDER BY valid_from NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_valid_to,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY tariff), 0), 4) AS rate_share_of_tariff,
        DENSE_RANK() OVER (PARTITION BY YEAR(valid_from), tariff ORDER BY minutes_valid_to_to_valid_from DESC) AS minutes_valid_to_to_valid_from_dense_rank,
        LAG(valid_to) OVER w_all_by_rate AS prev_valid_to,
        ROW_NUMBER() OVER (PARTITION BY YEAR(valid_from), tariff ORDER BY minutes_valid_to_to_valid_from DESC, id) AS minutes_valid_to_to_valid_from_seq,
        COUNT(*) OVER (PARTITION BY MONTH(valid_to), tariff) AS rows_in_valid_to_month_tariff,
        SUM(rate) OVER (ORDER BY valid_to ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rate,
        ROUND(AVG(rate) OVER w_valid_to_month_tariff, 2) AS valid_to_month_tariff_avg_rate,
        ROUND(rate - AVG(rate) OVER (PARTITION BY tariff), 2) AS rate_vs_tariff_avg
    FROM scoped_tariffs
    WINDOW w_all_by_rate AS (ORDER BY rate DESC, id), w_valid_to_month_tariff AS (PARTITION BY MONTH(valid_to), tariff)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(rate_vs_tariff_avg - AVG(rate_vs_tariff_avg) OVER (PARTITION BY tariff), 2) AS rate_vs_tariff_avg_vs_tariff_avg,
        LEAD(prev_valid_to) OVER w_tariff_by_running_rate AS next_prev_valid_to,
        MAX(rate_vs_tariff_avg) OVER (PARTITION BY DATE_TRUNC('MONTH', second_valid_to), DATE_TRUNC('MONTH', valid_to) ORDER BY rows_in_valid_to_month_tariff DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rate_vs_tariff_avg_max_3_prior,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY tariff), 0), 4) AS rate_share_of_tariff_2,
        ROUND(rate_share_of_tariff - AVG(rate_share_of_tariff) OVER (PARTITION BY tariff), 2) AS rate_share_of_tariff_vs_tariff_avg,
        ROUND(AVG(rate) OVER (PARTITION BY tariff), 2) AS tariff_avg_rate
    FROM windowed
    WINDOW w_tariff_by_running_rate AS (PARTITION BY tariff ORDER BY running_rate NULLS LAST, id)
)
SELECT
    id,
    tariff,
    valid_from,
    valid_to,
    second_valid_to,
    minutes_valid_to_to_valid_from_dense_rank,
    prev_valid_to,
    rows_in_valid_to_month_tariff,
    running_rate,
    valid_to_month_tariff_avg_rate,
    rate_vs_tariff_avg,
    rate_vs_tariff_avg_vs_tariff_avg,
    next_prev_valid_to,
    rate_share_of_tariff_2,
    rate_share_of_tariff_vs_tariff_avg,
    tariff_avg_rate
FROM metrics
