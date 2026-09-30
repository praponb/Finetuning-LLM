WITH valid_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        CASE WHEN tariff IN ('EV', 'FLAT') THEN 'group_a' WHEN tariff = 'TOU' THEN 'group_b' ELSE 'other' END AS tariff_group,
        SUBSTRING(TRIM(tariff), 1, 2) AS tariff_prefix,
        LENGTH(TRIM(tariff)) AS tariff_len
    FROM tariff_windows
    WHERE HOUR(valid_to) BETWEEN 6 AND 9
),
tariff_group_stats AS (
    -- Median rate per tariff_group
    SELECT tariff_group, PERCENTILE_APPROX(rate, 0.5) AS median_rate
    FROM valid_tariffs
    GROUP BY tariff_group
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_rate, ROUND(s.rate - g.median_rate, 2) AS rate_minus_median
    FROM valid_tariffs s
    JOIN tariff_group_stats g ON g.tariff_group = s.tariff_group
    WHERE s.rate >= g.median_rate
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(tariff_prefix)) AS tariff_prefix_len,
        DATEDIFF(DATE '2025-06-30', valid_to) AS days_since_valid_to
    FROM above_average
),
tariff_group_stats_2 AS (
    -- Avg rate_minus_median per tariff_group
    SELECT tariff_group, AVG(rate_minus_median) AS avg_rate_minus_median
    FROM derived
    GROUP BY tariff_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_rate_minus_median,
        ROUND(s.rate_minus_median - g.avg_rate_minus_median, 2) AS rate_minus_median_minus_avg
    FROM derived s
    JOIN tariff_group_stats_2 g ON g.tariff_group = s.tariff_group
    WHERE s.rate_minus_median > g.avg_rate_minus_median
),
tariff_group_stats_3 AS (
    -- Median tariff_prefix_len per tariff_group
    SELECT tariff_group, PERCENTILE_APPROX(tariff_prefix_len, 0.5) AS median_tariff_prefix_len
    FROM vs_group
    GROUP BY tariff_group
),
above_average_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_tariff_prefix_len,
        ROUND(s.tariff_prefix_len - g.median_tariff_prefix_len, 2) AS tariff_prefix_len_minus_median
    FROM vs_group s
    JOIN tariff_group_stats_3 g ON g.tariff_group = s.tariff_group
    WHERE s.tariff_prefix_len >= g.median_tariff_prefix_len
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM above_average_2
    WHERE tariff_len > (SELECT MIN(tariff_len) FROM above_average_2)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(rate_minus_median_minus_avg) OVER (ORDER BY days_since_valid_to, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS rate_minus_median_minus_avg_avg_2,
        ROUND(CUME_DIST() OVER w_all_by_days_since_valid_to, 3) AS days_since_valid_to_cume_dist,
        SIZE(COLLECT_SET(tariff_group) OVER w_valid_to_month_tariff_len) AS distinct_tariff_group_in_valid_to_month_tariff_l
    FROM with_activity
    WINDOW w_all_by_days_since_valid_to AS (ORDER BY days_since_valid_to, id), w_valid_to_month_tariff_len AS (PARTITION BY MONTH(valid_to), tariff_len)
)
SELECT *
FROM (
    SELECT valid_to, tariff_group, tariff_len
    FROM sequenced
) src
PIVOT (
    MIN(tariff_len)
    FOR tariff_group IN ('group_b', 'other')
)
