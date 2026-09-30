WITH prepared AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        ABS(rate - 5.7) AS rate_distance
    FROM tariff_windows
    WHERE tariff <> 'TOU'
      AND TO_DATE(valid_from) < DATE '2025-03-09'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY tariff_or_unknown, tariff ORDER BY rate DESC, id) AS rn,
        ROUND(AVG(rate_distance) OVER (PARTITION BY tariff, tariff_or_unknown), 2) AS tariff_tariff_or_unknown_avg_rate_distance,
        MIN(rate) OVER (PARTITION BY tariff_or_unknown, tariff) AS tariff_or_unknown_tariff_min_rate,
        COUNT(*) OVER w_tariff_tariff_or_unknown AS rows_in_tariff_tariff_or_unknown,
        LEAD(rate) OVER (ORDER BY rate_distance DESC NULLS FIRST, id) AS next_rate,
        rate_distance - LAG(rate_distance) OVER w_tariff_tariff_or_unknown_by AS rate_distance_change,
        COUNT(*) OVER (PARTITION BY tariff_or_unknown, tariff) AS rows_in_tariff_or_unknown_tariff
    FROM prepared
    WINDOW w_tariff_tariff_or_unknown AS (PARTITION BY tariff, tariff_or_unknown), w_tariff_tariff_or_unknown_by AS (PARTITION BY tariff, tariff_or_unknown ORDER BY valid_to DESC, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        tariff_or_unknown,
        rate_distance,
        tariff_tariff_or_unknown_avg_rate_distance,
        tariff_or_unknown_tariff_min_rate,
        rows_in_tariff_tariff_or_unknown,
        next_rate,
        rate_distance_change,
        rows_in_tariff_or_unknown_tariff
    FROM windowed
    WHERE rn = 1
)
SELECT
    id,
    tariff,
    rate,
    tariff_or_unknown,
    tariff_or_unknown_tariff_min_rate,
    next_rate,
    rate_distance_change,
    rows_in_tariff_or_unknown_tariff,
    CASE WHEN next_rate >= 5.7 THEN 'HIGH' WHEN next_rate >= 3.0 THEN 'MEDIUM' ELSE 'LOW' END AS next_rate_tier
FROM leaders
ORDER BY tariff_or_unknown DESC
