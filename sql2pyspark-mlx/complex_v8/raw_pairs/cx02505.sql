WITH scoped AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        valid_from + INTERVAL 15 MINUTES AS valid_from_plus_15m,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        TO_DATE(valid_to) AS valid_to_date
    FROM tariff_windows
    WHERE tariff <> 'TOU'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(valid_from_plus_15m)) / 60, 1) AS minutes_valid_from_plus_15m_to_valid_to,
        TO_DATE(valid_from) AS valid_from_date
    FROM scoped
),
annotated AS (
    -- Derive helper columns
    SELECT *, INITCAP(TRIM(tariff)) AS tariff_clean
    FROM tagged
),
tariff_stats AS (
    -- Avg rate per tariff
    SELECT tariff, AVG(rate) AS avg_rate
    FROM annotated
    GROUP BY tariff
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rate, ROUND(s.rate - g.avg_rate, 2) AS rate_minus_avg
    FROM annotated s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.rate > g.avg_rate
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, minutes_valid_from_plus_15m_to_valid_to, rate, avg_rate
    FROM vs_group
) src
UNPIVOT (
    metric_value FOR measure IN (minutes_valid_from_plus_15m_to_valid_to, rate, avg_rate)
) unp
