WITH cleaned AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        valid_from + INTERVAL 1 DAYS AS valid_from_plus_1d,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown_2
    FROM tariff_windows
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        valid_from_plus_1d,
        tariff_or_unknown_2,
        SUM(rate) OVER w_valid_from_plus_1d_month_tar AS valid_from_plus_1d_month_tariff_sum_rate,
        ROUND(AVG(rate) OVER w_valid_from_plus_1d_month_tar, 2) AS valid_from_plus_1d_month_tariff_avg_rate,
        RANK() OVER w_tariff_by_valid_from AS valid_from_rank,
        DENSE_RANK() OVER w_tariff_by_valid_from_ties AS valid_from_dense_rank,
        ROUND(rate - AVG(rate) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from_plus_1d), tariff), 2) AS rate_vs_valid_from_plus_1d_month_tariff_avg
    FROM cleaned
    WINDOW w_valid_from_plus_1d_month_tar AS (PARTITION BY DATE_TRUNC('MONTH', valid_from_plus_1d), tariff), w_tariff_by_valid_from AS (PARTITION BY tariff ORDER BY valid_from DESC, id), w_tariff_by_valid_from_ties AS (PARTITION BY tariff ORDER BY valid_from DESC)
),
scored AS (
    -- Add calculated columns
    SELECT *, COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM windowed
    WHERE valid_from_rank > 5
),
tariff_stats AS (
    -- Max valid_from_plus_1d_month_tariff_avg_rate per tariff
    SELECT tariff, MAX(valid_from_plus_1d_month_tariff_avg_rate) AS max_valid_from_plus_1d_month_tariff_avg_rate
    FROM scored
    GROUP BY tariff
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_valid_from_plus_1d_month_tariff_avg_rate,
        ROUND(s.valid_from_plus_1d_month_tariff_avg_rate - g.max_valid_from_plus_1d_month_tariff_avg_rate, 2) AS valid_from_plus_1d_month_tariff_avg_rate_minus_m
    FROM scored s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.valid_from_plus_1d_month_tariff_avg_rate = g.max_valid_from_plus_1d_month_tariff_avg_rate
),
latest_per_key AS (
    -- Keep the latest row per tariff_or_unknown, tariff_or_unknown_2
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        valid_from_plus_1d,
        tariff_or_unknown_2,
        valid_from_plus_1d_month_tariff_sum_rate,
        valid_from_plus_1d_month_tariff_avg_rate,
        valid_from_rank,
        valid_from_dense_rank,
        rate_vs_valid_from_plus_1d_month_tariff_avg,
        tariff_or_unknown,
        max_valid_from_plus_1d_month_tariff_avg_rate,
        valid_from_plus_1d_month_tariff_avg_rate_minus_m
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff_or_unknown, tariff_or_unknown_2 ORDER BY valid_from DESC, id DESC) AS rn
        FROM vs_group
    ) ranked
    WHERE rn = 1
)
SELECT
    tariff_or_unknown_2,
    valid_from_dense_rank,
    tariff_or_unknown,
    MIN(valid_to) AS first_valid_to
FROM latest_per_key
GROUP BY GROUPING SETS ((tariff_or_unknown_2, valid_from_dense_rank, tariff_or_unknown), (tariff_or_unknown_2), (valid_from_dense_rank), ())
ORDER BY tariff_or_unknown_2 NULLS LAST, valid_from_dense_rank NULLS LAST, tariff_or_unknown NULLS LAST
