WITH valid_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        TO_DATE(valid_from) AS valid_from_date,
        SUBSTRING(TRIM(tariff), 1, 2) AS tariff_prefix,
        DATE_FORMAT(valid_from, 'yyyy-MM-dd HH:00') AS valid_from_label
    FROM tariff_windows
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(rate) OVER (PARTITION BY tariff, MONTH(valid_from_date) ORDER BY rate DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS rate_avg_2,
        RANK() OVER w_tariff_valid_from_date_month_2 AS rate_rank
    FROM valid_tariffs
    WINDOW w_tariff_valid_from_date_month_2 AS (PARTITION BY tariff, MONTH(valid_from_date) ORDER BY rate DESC)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY rate_rank ORDER BY valid_from_date, id) AS rn,
        NTH_VALUE(rate_rank, 2) OVER (ORDER BY rate_avg_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rate_rank,
        MAX(rate_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from_date), tariff) AS valid_from_date_month_tariff_max_rate_rank
    FROM sequenced
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        valid_from_date,
        rate_rank,
        second_rate_rank,
        valid_from_date_month_tariff_max_rate_rank,
        MIN(rate_rank) OVER (PARTITION BY TO_DATE(valid_from), tariff) AS valid_from_to_date_tariff_min_rate_rank,
        NTILE(10) OVER (PARTITION BY rate_rank ORDER BY rate_avg_2 DESC, id) AS rate_avg_2_decile,
        SUM(rate) OVER (PARTITION BY rate_rank ORDER BY rate_avg_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rate,
        COUNT(*) OVER w_valid_from_to_date_tariff AS rows_in_valid_from_to_date_tariff,
        ROW_NUMBER() OVER (PARTITION BY rate_rank ORDER BY rate_avg_2 DESC, id) AS rate_avg_2_seq
    FROM with_trends
    WINDOW w_valid_from_to_date_tariff AS (PARTITION BY TO_DATE(valid_from), tariff)
)
SELECT id, field, amount
FROM (
    SELECT id, rate, running_rate
    FROM metrics
) src
UNPIVOT (
    amount FOR field IN (rate, running_rate)
) unp
ORDER BY id, field
