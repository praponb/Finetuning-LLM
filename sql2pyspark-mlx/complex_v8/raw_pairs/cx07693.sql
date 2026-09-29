WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        DATE_FORMAT(valid_to, 'yyyy-MM-dd HH:00') AS valid_to_label
    FROM tariff_windows
    WHERE tariff = 'TOU'
      AND valid_from >= TIMESTAMP '2025-01-23 19:00:00'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(rate) OVER (PARTITION BY YEAR(valid_to), tariff) AS valid_to_year_tariff_sum_rate,
        LAG(rate) OVER (PARTITION BY tariff, MONTH(valid_to) ORDER BY valid_from, id) AS prev_rate,
        COUNT(*) OVER (PARTITION BY tariff, MONTH(valid_to) ORDER BY valid_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(rate - AVG(rate) OVER w_valid_to_year_tariff, 2) AS rate_vs_valid_to_year_tariff_avg,
        ROUND(rate / NULLIF(SUM(rate) OVER w_valid_to_year_tariff, 0), 4) AS rate_share_of_valid_to_year_tariff,
        NTH_VALUE(valid_to, 2) OVER (PARTITION BY tariff ORDER BY rate, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_valid_to,
        ROUND(CUME_DIST() OVER (PARTITION BY tariff, MONTH(valid_to) ORDER BY valid_from), 3) AS valid_from_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY rate, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY MONTH(valid_from), tariff ORDER BY rate DESC NULLS FIRST, id) AS rate_seq,
        FIRST_VALUE(valid_to) OVER (PARTITION BY MONTH(valid_from), tariff ORDER BY rate DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_valid_to,
        NTILE(10) OVER w_tariff_by_rate AS rate_decile,
        ROUND(AVG(rate) OVER (PARTITION BY tariff, MONTH(valid_to) ORDER BY valid_from, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS rate_avg_3_prior,
        LEAD(valid_to) OVER w_valid_from_month_tariff_by_r AS next_valid_to,
        LEAD(valid_to, 2) OVER w_tariff_by_rate AS next2_valid_to
    FROM valid
    WINDOW w_valid_to_year_tariff AS (PARTITION BY YEAR(valid_to), tariff), w_tariff_by_rate AS (PARTITION BY tariff ORDER BY rate, id), w_valid_from_month_tariff_by_r AS (PARTITION BY MONTH(valid_from), tariff ORDER BY rate DESC NULLS FIRST, id), w_tariff_valid_to_month_by_val_3 AS (PARTITION BY tariff, MONTH(valid_to) ORDER BY valid_from)
)
-- Final projection
SELECT
    id,
    valid_from,
    valid_to,
    rate,
    valid_to_label,
    prev_rate,
    running_count,
    rate_vs_valid_to_year_tariff_avg,
    rate_share_of_valid_to_year_tariff,
    second_valid_to,
    valid_from_cume_dist,
    seq_no,
    rate_seq,
    rate_decile,
    rate_avg_3_prior,
    next_valid_to,
    next2_valid_to
FROM with_trends
ORDER BY rate_vs_valid_to_year_tariff_avg, prev_rate DESC
