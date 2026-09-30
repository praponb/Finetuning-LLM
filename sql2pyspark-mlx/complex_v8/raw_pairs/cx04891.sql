WITH prepared_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_to,
        rate
    FROM tariff_windows
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        MAX(rate) OVER (PARTITION BY tariff) AS tariff_max_rate,
        SUM(rate) OVER (PARTITION BY TO_DATE(valid_to) ORDER BY valid_to ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rate,
        NTH_VALUE(valid_to, 2) OVER (PARTITION BY TO_DATE(valid_to) ORDER BY valid_to ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_valid_to,
        SUM(rate) OVER (PARTITION BY tariff) AS tariff_sum_rate
    FROM prepared_tariffs
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        tariff,
        CASE WHEN tariff_max_rate >= 52.7 THEN 'A' WHEN tariff_max_rate >= 29.3 THEN 'B' ELSE 'C' END AS tariff_max_rate_tier,
        TO_DATE(second_valid_to) AS second_valid_to_day,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        SUM(CASE WHEN tariff <> 'FLAT' THEN tariff_sum_rate ELSE 0 END) AS tariff_sum_rate_tariff_flat,
        ROUND(SUM(running_rate), 2) AS total_running_rate,
        ROUND(SUM(tariff_sum_rate) / NULLIF(SUM(running_rate), 0), 3) AS tariff_sum_rate_to_running_rate_ratio
    FROM with_trends
    GROUP BY tariff, CASE WHEN tariff_max_rate >= 52.7 THEN 'A' WHEN tariff_max_rate >= 29.3 THEN 'B' ELSE 'C' END, TO_DATE(second_valid_to)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT cnt, distinct_ids, tariff, tariff_sum_rate_tariff_flat
    FROM grouped
) src
PIVOT (
    SUM(tariff_sum_rate_tariff_flat)
    FOR tariff IN ('TOU', 'FLAT')
)
