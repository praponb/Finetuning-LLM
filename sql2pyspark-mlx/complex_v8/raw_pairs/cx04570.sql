WITH cleaned_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM tariff_windows
    WHERE (HOUR(valid_to) BETWEEN 9 AND 13 OR tariff IN ('EV', 'TOU'))
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('|', tariff_or_unknown, tariff) AS tariff_or_unknown_tariff_label
    FROM cleaned_tariffs
    WHERE tariff_or_unknown = 'TOU'
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched
    WHERE rate > (SELECT AVG(rate) FROM enriched)
),
tariff_stats AS (
    -- Max rate per tariff
    SELECT tariff, MAX(rate) AS max_rate
    FROM qualified
    GROUP BY tariff
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_rate, ROUND(s.rate - g.max_rate, 2) AS rate_minus_max
    FROM qualified s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.rate = g.max_rate
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        tariff_or_unknown,
        DATE_TRUNC('MONTH', valid_from) AS valid_from_month,
        MIN(rate_minus_max) AS min_rate_minus_max,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(max_rate, 0.5) AS median_max_rate,
        ROUND(STDDEV(rate_minus_max), 2) AS stddev_rate_minus_max,
        MAX(rate_minus_max) AS max_rate_minus_max,
        MAX(rate) AS max_rate_2
    FROM vs_group
    WHERE rate_minus_max < 1.6
    GROUP BY tariff_or_unknown, DATE_TRUNC('MONTH', valid_from)
)
SELECT tariff_or_unknown, valid_from_month, metric, amount
FROM (
    SELECT tariff_or_unknown, valid_from_month, max_rate_minus_max, max_rate_2
    FROM per_group
) src
UNPIVOT (
    amount FOR metric IN (max_rate_minus_max AS m_max_rate_minus_max, max_rate_2 AS m_max_rate_2)
) unp
