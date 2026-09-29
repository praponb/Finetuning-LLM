WITH filtered AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        QUARTER(valid_to) AS valid_to_quarter,
        valid_to + INTERVAL 7 DAYS AS valid_to_plus_7d
    FROM tariff_windows
    WHERE (HOUR(valid_to) BETWEEN 7 AND 12 OR tariff = 'FLAT')
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        tariff,
        MIN(rate) AS min_rate,
        MAX(rate) AS max_rate,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(rate) AS total_rate
    FROM filtered
    GROUP BY tariff
)
-- Native SQL UNPIVOT (wide to long)
SELECT tariff, field, metric_value
FROM (
    SELECT tariff, max_rate, min_rate
    FROM per_group
) src
UNPIVOT (
    metric_value FOR field IN (max_rate AS m_max_rate, min_rate AS min_rate_v)
) unp
ORDER BY tariff, field
