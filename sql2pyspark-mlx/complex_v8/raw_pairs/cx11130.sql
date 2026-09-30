WITH scoped_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        CASE WHEN tariff IN ('FLAT', 'TOU') THEN 'group_a' WHEN tariff = 'EV' THEN 'group_b' ELSE 'other' END AS tariff_group,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM tariff_windows
    WHERE (TO_DATE(valid_to) < DATE '2025-04-05' OR rate > 3.0)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        tariff_ref,
        tariff_slug,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(LAG(valid_from) OVER w_all_by_rate)) / 60, 1) AS minutes_since_prev_valid_from,
        LEAD(valid_from, 2) OVER (ORDER BY rate, id) AS next2_valid_from,
        DENSE_RANK() OVER w_all_by_rate_ties AS rate_dense_rank
    FROM scoped_tariffs
    WINDOW w_all_by_rate AS (ORDER BY rate, id), w_all_by_rate_ties AS (ORDER BY rate)
),
tariff_stats AS (
    -- Avg rate per tariff
    SELECT tariff, AVG(rate) AS avg_rate
    FROM windowed
    GROUP BY tariff
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rate, ROUND(s.rate - g.avg_rate, 2) AS rate_minus_avg
    FROM windowed s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.rate > g.avg_rate
),
tariff_stats_2 AS (
    -- Median rate per tariff
    SELECT tariff, PERCENTILE_APPROX(rate, 0.5) AS median_rate
    FROM compared
    GROUP BY tariff
),
compared_2 AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_rate, ROUND(s.rate - g.median_rate, 2) AS rate_minus_median
    FROM compared s
    JOIN tariff_stats_2 g ON g.tariff = s.tariff
    WHERE s.rate >= g.median_rate
)
SELECT id, measure, metric_value
FROM (
    SELECT id, minutes_since_prev_valid_from, rate_minus_avg, median_rate, rate_minus_median
    FROM compared_2
) src
UNPIVOT (
    metric_value FOR measure IN (minutes_since_prev_valid_from AS minutes_since_prev_valid_from_v, rate_minus_avg AS m_rate_minus_avg, median_rate AS median_rate_v, rate_minus_median AS rate_minus_median_v)
) unp
