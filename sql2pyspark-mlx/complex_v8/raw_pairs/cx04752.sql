WITH prepared AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE (rate BETWEEN 2.0 AND 5.7 OR HOUR(valid_to) BETWEEN 8 AND 11)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        ROUND(PERCENT_RANK() OVER (ORDER BY valid_from DESC NULLS FIRST, id), 3) AS valid_from_percent_rank,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY YEAR(valid_from), tariff), 0), 4) AS rate_share_of_valid_from_year_tariff,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(LAG(valid_from) OVER (PARTITION BY tariff, MONTH(valid_from) ORDER BY valid_from DESC, id))) / 60, 1) AS minutes_since_prev_valid_from,
        COUNT(*) OVER (PARTITION BY YEAR(valid_from), tariff) AS rows_in_valid_from_year_tariff,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(LAG(valid_to) OVER (PARTITION BY tariff, MONTH(valid_from) ORDER BY valid_from DESC, id))) / 60, 1) AS minutes_since_prev_valid_to
    FROM prepared
),
grouped AS (
    -- Summarise per group
    SELECT
        tariff,
        MIN(minutes_since_prev_valid_from) AS min_minutes_since_prev_valid_from,
        COUNT(DISTINCT tariff) AS distinct_tariffs,
        MAX(rate_share_of_valid_from_year_tariff) AS max_rate_share_of_valid_from_year_tariff
    FROM with_trends
    GROUP BY tariff
)
SELECT tariff, metric_name, reading
FROM (
    SELECT tariff, min_minutes_since_prev_valid_from, max_rate_share_of_valid_from_year_tariff
    FROM grouped
) src
UNPIVOT (
    reading FOR metric_name IN (min_minutes_since_prev_valid_from, max_rate_share_of_valid_from_year_tariff)
) unp
