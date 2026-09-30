WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        ROUND(rate * 0.9, 2) AS rate_adj,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref
    FROM tariff_windows
    WHERE tariff = 'TOU'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(valid_from) AS valid_from_year,
        tariff,
        MAX(valid_from) AS last_valid_from,
        ROUND(SUM(rate_adj), 2) AS total_rate_adj,
        SUM(CASE WHEN tariff IS NOT NULL THEN 1 ELSE 0 END) AS n_tariff_is_not_null,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list,
        SUM(CASE WHEN tariff = 'FLAT' THEN 1 ELSE 0 END) AS n_tariff_flat,
        SUM(rate) FILTER (WHERE tariff <> 'EV') AS rate_tariff_ev
    FROM filtered
    WHERE HOUR(valid_from) BETWEEN 6 AND 12
    GROUP BY YEAR(valid_from), tariff
    HAVING AVG(rate) > 4.2
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(n_tariff_is_not_null, rate_tariff_ev) AS least_n_tariff_is_not_null_rate_tariff_ev,
        total_rate_adj - n_tariff_is_not_null AS total_rate_adj_minus_n_tariff_is_not_null
    FROM stats
),
enriched_2 AS (
    -- Derive helper columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM enriched
    WHERE rate_tariff_ev >= 14.0
)
-- Final projection
SELECT
    valid_from_year,
    tariff,
    last_valid_from,
    total_rate_adj,
    n_tariff_is_not_null,
    tariff_list,
    rate_tariff_ev,
    least_n_tariff_is_not_null_rate_tariff_ev,
    total_rate_adj_minus_n_tariff_is_not_null
FROM enriched_2
ORDER BY tariff, valid_from_year
LIMIT 5
