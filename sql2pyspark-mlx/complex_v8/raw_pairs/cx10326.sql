WITH prepared_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        DATE_FORMAT(valid_from, 'yyyy-MM-dd') AS valid_from_label
    FROM tariff_windows
    WHERE rate < 6.1
      AND tariff = 'TOU'
),
tariff_stats AS (
    -- Avg rate per tariff
    SELECT tariff, AVG(rate) AS avg_rate
    FROM prepared_tariffs
    GROUP BY tariff
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rate, ROUND(s.rate - g.avg_rate, 2) AS rate_minus_avg
    FROM prepared_tariffs s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.rate > g.avg_rate
),
totals AS (
    -- Group-level totals
    SELECT
        CASE WHEN avg_rate >= 5.3 THEN 'large' WHEN avg_rate >= 3.8 THEN 'medium' ELSE 'small' END AS avg_rate_tier,
        DATE_TRUNC('MONTH', valid_from) AS valid_from_month,
        tariff,
        COUNT(*) FILTER (WHERE tariff <> 'TOU') AS n_tariff_tou,
        MIN(valid_from) AS first_valid_from,
        COUNT(*) AS row_count
    FROM above_average
    GROUP BY CASE WHEN avg_rate >= 5.3 THEN 'large' WHEN avg_rate >= 3.8 THEN 'medium' ELSE 'small' END, DATE_TRUNC('MONTH', valid_from), tariff
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', first_valid_from) AS days_since_first_valid_from,
        ROUND(n_tariff_tou / NULLIF(row_count, 0), 3) AS n_tariff_tou_per_row_count
    FROM totals
    WHERE row_count <= 9
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        avg_rate_tier,
        valid_from_month,
        tariff,
        n_tariff_tou,
        first_valid_from,
        n_tariff_tou_per_row_count,
        ROUND(n_tariff_tou - AVG(n_tariff_tou) OVER (PARTITION BY avg_rate_tier, DATE_TRUNC('MONTH', valid_from_month)), 2) AS n_tariff_tou_vs_avg_rate_tier_valid_from_month_m,
        ROUND(CUME_DIST() OVER (PARTITION BY n_tariff_tou, avg_rate_tier ORDER BY n_tariff_tou_per_row_count ASC), 3) AS n_tariff_tou_per_row_count_cume_dist,
        LAST_VALUE(valid_from_month) IGNORE NULLS OVER (PARTITION BY n_tariff_tou, avg_rate_tier ORDER BY n_tariff_tou_per_row_count ASC, avg_rate_tier, valid_from_month, tariff ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_valid_from_month,
        SUM(row_count) OVER (PARTITION BY TO_DATE(first_valid_from) ORDER BY days_since_first_valid_from DESC, avg_rate_tier, valid_from_month, tariff ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS row_count_sum_3_prior
    FROM derived
)
SELECT
    avg_rate_tier,
    valid_from_month,
    tariff,
    n_tariff_tou,
    first_valid_from,
    n_tariff_tou_per_row_count_cume_dist
FROM with_windows
ORDER BY avg_rate_tier DESC
