WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(valid_to)) / 3600, 1) AS hours_valid_to_to_valid_from,
        DATE_FORMAT(valid_to, 'MMM yyyy') AS valid_to_label
    FROM tariff_windows
    WHERE valid_to >= TIMESTAMP '2025-02-16 05:00:00'
),
tariff_stats AS (
    -- Avg hours_valid_to_to_valid_from per tariff
    SELECT tariff, AVG(hours_valid_to_to_valid_from) AS avg_hours_valid_to_to_valid_from
    FROM base
    GROUP BY tariff
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_hours_valid_to_to_valid_from,
        ROUND(s.hours_valid_to_to_valid_from - g.avg_hours_valid_to_to_valid_from, 2) AS hours_valid_to_to_valid_from_minus_avg
    FROM base s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.hours_valid_to_to_valid_from > g.avg_hours_valid_to_to_valid_from
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        hours_valid_to_to_valid_from,
        valid_to_label,
        hours_valid_to_to_valid_from_minus_avg,
        CONCAT_WS(': ', tariff, valid_to_label) AS tariff_valid_to_label_label,
        CASE WHEN rate >= 5.9 THEN 'HIGH' WHEN rate >= 4.2 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier,
        LOWER(REGEXP_REPLACE(TRIM(valid_to_label), ' ', '_')) AS valid_to_label_slug
    FROM above_average
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE rate > (SELECT AVG(rate) FROM derived)
)
SELECT *
FROM (
    SELECT valid_from, rate_tier, rate
    FROM without_matches
) src
PIVOT (
    AVG(rate)
    FOR rate_tier IN ('HIGH', 'MEDIUM')
)
ORDER BY valid_from
