WITH base_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        LOWER(TRIM(tariff)) AS tariff_clean,
        COALESCE(rate, 0.0) AS rate_filled
    FROM tariff_windows
    WHERE tariff IS NOT NULL
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM base_tariffs
    WHERE rate > (SELECT AVG(rate) FROM base_tariffs)
),
screened_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM screened
    WHERE rate < (SELECT MAX(rate) FROM screened)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        tariff_clean,
        ROUND(rate_filled * 0.25, 2) AS rate_filled_adj,
        COALESCE(rate_filled, 0.0) AS rate_filled_filled,
        DATE_FORMAT(valid_from, 'yyyy-MM-dd') AS valid_from_label
    FROM screened_2
    WHERE rate <= 5.7
)
SELECT id, measure, reading
FROM (
    SELECT id, rate_filled_adj, rate_filled_filled, rate
    FROM tagged
) src
UNPIVOT (
    reading FOR measure IN (rate_filled_adj, rate_filled_filled, rate)
) unp
