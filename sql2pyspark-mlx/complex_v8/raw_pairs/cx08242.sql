WITH filtered_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        ROUND(rate * 1.1, 2) AS rate_adj,
        TO_DATE(valid_to) AS valid_to_date,
        DATE_FORMAT(valid_from, 'MMM yyyy') AS valid_from_label
    FROM tariff_windows
    WHERE tariff IN ('EV', 'FLAT')
      AND valid_from < TIMESTAMP '2025-02-16 11:00:00'
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered_tariffs
    WHERE rate_adj > (SELECT AVG(rate_adj) FROM filtered_tariffs)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT valid_from, valid_to, tariff, rate
    FROM screened
) src
PIVOT (
    MIN(rate)
    FOR tariff IN ('EV', 'TOU')
)
