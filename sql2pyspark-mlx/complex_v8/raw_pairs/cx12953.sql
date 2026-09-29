WITH filtered_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        LENGTH(TRIM(tariff)) AS tariff_len,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM tariff_windows
    WHERE valid_to < TIMESTAMP '2025-03-12 23:00:00'
      AND rate > 4.2
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        tariff_len,
        MAX(rate) OVER (PARTITION BY MONTH(valid_to), tariff_len ORDER BY valid_to NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_rate_so_far,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(valid_to) ORDER BY valid_to DESC) AS valid_to_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tariff_len, tariff_or_unknown ORDER BY valid_from, id), 3) AS valid_from_percent_rank,
        MAX(tariff_len) OVER (ORDER BY valid_from DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS tariff_len_max_1_prior,
        LEAD(rate) OVER (ORDER BY valid_from DESC, id) AS next_rate,
        MAX(rate) OVER w_valid_from_month AS valid_from_month_max_rate,
        SIZE(COLLECT_SET(tariff_or_unknown) OVER w_tariff_len) AS distinct_tariff_or_unknown_in_tariff_len,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY MONTH(valid_from)), 0), 4) AS rate_share_of_valid_from_month,
        LEAD(valid_from, 2) OVER (ORDER BY valid_from DESC, id) AS next2_valid_from,
        SUM(tariff_len) OVER (ORDER BY valid_from DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS tariff_len_sum_3,
        RANK() OVER (PARTITION BY MONTH(valid_to), tariff_len ORDER BY valid_to NULLS LAST, id) AS valid_to_rank,
        LEAD(valid_from) OVER (ORDER BY valid_from DESC, id) AS next_valid_from,
        ROUND(AVG(tariff_len) OVER (PARTITION BY MONTH(valid_from)), 2) AS valid_from_month_avg_tariff_len,
        FIRST_VALUE(tariff_len) IGNORE NULLS OVER (ORDER BY rate ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_tariff_len,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(valid_to), tariff_len ORDER BY valid_to NULLS LAST), 3) AS valid_to_cume_dist,
        MIN(tariff_len) OVER w_tariff_or_unknown AS tariff_or_unknown_min_tariff_len
    FROM filtered_tariffs
    WINDOW w_valid_from_month AS (PARTITION BY MONTH(valid_from)), w_tariff_len AS (PARTITION BY tariff_len), w_tariff_or_unknown AS (PARTITION BY tariff_or_unknown)
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        valid_from,
        valid_to,
        tariff_len,
        max_rate_so_far,
        valid_to_dense_rank,
        valid_from_percent_rank,
        tariff_len_max_1_prior,
        next_rate,
        distinct_tariff_or_unknown_in_tariff_len,
        rate_share_of_valid_from_month,
        next2_valid_from,
        tariff_len_sum_3,
        valid_to_rank,
        next_valid_from,
        valid_from_month_avg_tariff_len,
        valid_to_cume_dist,
        tariff_or_unknown_min_tariff_len,
        RANK() OVER (PARTITION BY TO_DATE(next_valid_from) ORDER BY rate, id) AS rate_rank,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_to), tariff_len) AS rows_in_valid_to_month_tariff_len,
        ROUND(AVG(valid_to_cume_dist) OVER (ORDER BY distinct_tariff_or_unknown_in_tariff_len ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS valid_to_cume_dist_avg_1_prior
    FROM with_windows
)
-- Final projection
SELECT
    id,
    valid_from,
    valid_to,
    tariff_len,
    max_rate_so_far,
    valid_from_percent_rank,
    tariff_len_max_1_prior,
    rate_share_of_valid_from_month,
    next2_valid_from,
    tariff_len_sum_3,
    valid_to_rank,
    valid_to_cume_dist,
    tariff_or_unknown_min_tariff_len,
    rate_rank,
    rows_in_valid_to_month_tariff_len,
    valid_to_cume_dist_avg_1_prior,
    YEAR(next2_valid_from) AS next2_valid_from_year
FROM with_windows_2
ORDER BY valid_to_cume_dist, max_rate_so_far, id
LIMIT 10
