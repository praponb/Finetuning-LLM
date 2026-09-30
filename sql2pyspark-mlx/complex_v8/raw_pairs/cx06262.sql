WITH cleaned_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        ROUND(rate * 1.1, 2) AS rate_adj
    FROM tariff_windows
    WHERE tariff = 'TOU'
      AND valid_from < TIMESTAMP '2025-02-18 04:00:00'
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned_tariffs
    WHERE rate > (SELECT AVG(rate) FROM cleaned_tariffs)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM without_matches
    WHERE rate > (SELECT MIN(rate) FROM without_matches)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        rate_adj,
        COALESCE(rate, 0.0) AS rate_filled
    FROM screened
)
-- Final projection
SELECT
    id,
    valid_to,
    rate,
    rate_adj,
    rate_filled,
    TO_DATE(valid_from) AS valid_from_date
FROM enriched
WHERE rate BETWEEN 3.0 AND 7.6
ORDER BY rate
