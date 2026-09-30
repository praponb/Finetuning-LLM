WITH cleaned AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        DATE_TRUNC('DAY', valid_to) AS valid_to_day
    FROM tariff_windows
    WHERE HOUR(valid_to) BETWEEN 6 AND 10
      AND HOUR(valid_from) BETWEEN 10 AND 14
),
tariff_stats AS (
    -- Median rate per tariff
    SELECT tariff, PERCENTILE_APPROX(rate, 0.5) AS median_rate
    FROM cleaned
    GROUP BY tariff
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_rate, ROUND(s.rate - g.median_rate, 2) AS rate_minus_median
    FROM cleaned s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.rate >= g.median_rate
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_tariff_by_rate_ties, 3) AS rate_cume_dist,
        ROUND(AVG(rate_minus_median) OVER (PARTITION BY tariff ORDER BY rate ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rate_minus_median_avg_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tariff ORDER BY rate ASC), 3) AS rate_percent_rank,
        LAG(valid_to) OVER (PARTITION BY tariff ORDER BY rate ASC, id) AS prev_valid_to
    FROM above_average
    WINDOW w_tariff_by_rate_ties AS (PARTITION BY tariff ORDER BY rate ASC)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', valid_to_day) AS days_since_valid_to_day,
        LENGTH(TRIM(tariff)) AS tariff_len,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM ranked
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(rate_minus_median / NULLIF(SUM(rate_minus_median) OVER (PARTITION BY tariff), 0), 4) AS rate_minus_median_share_of_tariff,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY tariff), 0), 4) AS rate_share_of_tariff
    FROM annotated
)
-- Final projection
SELECT
    id,
    tariff,
    valid_to,
    rate,
    median_rate,
    rate_minus_median,
    rate_cume_dist,
    rate_percent_rank,
    prev_valid_to,
    days_since_valid_to_day,
    tariff_len,
    tariff_or_unknown,
    rate_share_of_tariff,
    ROUND((UNIX_TIMESTAMP(prev_valid_to) - UNIX_TIMESTAMP(valid_to)) / 3600, 1) AS hours_valid_to_to_prev_valid_to
FROM windowed
ORDER BY rate
