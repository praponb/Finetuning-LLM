WITH base_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM tariff_windows
    WHERE (valid_to < TIMESTAMP '2025-03-12 23:00:00' OR rate >= 3.7)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM base_tariffs
    WHERE rate > (SELECT AVG(rate) FROM base_tariffs)
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM with_activity
    WHERE rate > (SELECT MIN(rate) FROM with_activity)
),
latest_per_key AS (
    -- Keep the latest row per tariff_or_unknown
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        tariff_ref,
        tariff_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff_or_unknown ORDER BY valid_to DESC, id DESC) AS recency_rank
        FROM qualified
    ) ranked
    WHERE recency_rank = 1
),
tariff_stats AS (
    -- Avg rate per tariff
    SELECT tariff, AVG(rate) AS avg_rate
    FROM latest_per_key
    GROUP BY tariff
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rate, ROUND(s.rate - g.avg_rate, 2) AS rate_minus_avg
    FROM latest_per_key s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.rate > g.avg_rate
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ABS(avg_rate - 5.9) AS avg_rate_distance,
        INITCAP(TRIM(tariff)) AS tariff_clean
    FROM compared
    WHERE tariff_ref IS NOT NULL
),
totals AS (
    -- Group-level totals
    SELECT
        tariff,
        DATE_TRUNC('MONTH', valid_from) AS valid_from_month,
        tariff_or_unknown,
        MAX(valid_to) AS last_valid_to,
        ROUND(AVG(rate_minus_avg), 2) AS avg_rate_minus_avg,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(rate_minus_avg) FILTER (WHERE tariff <> 'FLAT') AS rate_minus_avg_tariff_flat
    FROM derived
    GROUP BY tariff, DATE_TRUNC('MONTH', valid_from), tariff_or_unknown
)
-- Final projection
SELECT
    tariff,
    valid_from_month,
    distinct_ids,
    rate_minus_avg_tariff_flat,
    LENGTH(TRIM(tariff)) AS tariff_len,
    DAYOFWEEK(last_valid_to) AS last_valid_to_dayofweek
FROM totals
WHERE MONTH(valid_from_month) IN (2, 3)
ORDER BY last_valid_to_dayofweek DESC NULLS FIRST, rate_minus_avg_tariff_flat DESC
