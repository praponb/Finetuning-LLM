WITH filtered AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        COALESCE(rate, 0.0) AS rate_filled,
        DATE_FORMAT(valid_from, 'yyyy-MM-dd HH:00') AS valid_from_label,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM tariff_windows
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(valid_from) AS valid_from_day,
        tariff,
        COUNT(DISTINCT tariff) AS distinct_tariffs,
        SUM(CASE WHEN tariff NOT IN ('FLAT', 'TOU') THEN 1 ELSE 0 END) AS n_tariff_not_in_flat_tou,
        SUM(CASE WHEN tariff = 'EV' THEN rate ELSE 0 END) AS rate_tariff_ev,
        ROUND(STDDEV(rate), 2) AS stddev_rate,
        ROUND(STDDEV(rate_filled), 2) AS stddev_rate_filled,
        ROUND(AVG(rate), 2) AS avg_rate
    FROM filtered
    WHERE valid_from_label IS NOT NULL
    GROUP BY TO_DATE(valid_from), tariff
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT valid_from_day, tariff, distinct_tariffs
    FROM grouped
) src
PIVOT (
    COUNT(distinct_tariffs)
    FOR tariff IN ('EV', 'TOU')
)
