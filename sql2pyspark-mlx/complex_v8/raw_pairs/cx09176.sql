WITH recent AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        DATE_TRUNC('YEAR', valid_to) AS valid_to_year,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM tariff_windows
    WHERE (tariff <> 'FLAT' OR MONTH(valid_from) IN (2, 3))
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        tariff,
        rate,
        valid_to_year,
        CONCAT(tariff_or_unknown, '#', CAST(id AS STRING)) AS tariff_or_unknown_ref,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(valid_to_year)) / 60, 1) AS minutes_valid_to_year_to_valid_to,
        COALESCE(rate, 0.0) AS rate_filled
    FROM recent
    WHERE tariff_or_unknown IN ('UNKNOWN', 'EV')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(rate) OVER (PARTITION BY tariff ORDER BY minutes_valid_to_year_to_valid_to, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rate,
        ROUND(rate_filled / NULLIF(SUM(rate_filled) OVER (PARTITION BY tariff), 0), 4) AS rate_filled_share_of_tariff
    FROM scored
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked
    WHERE minutes_valid_to_year_to_valid_to > (SELECT AVG(minutes_valid_to_year_to_valid_to) FROM ranked)
)
-- Final projection
SELECT
    id,
    tariff,
    rate,
    valid_to_year,
    tariff_or_unknown_ref,
    rate_filled,
    last_rate,
    rate_filled_share_of_tariff,
    last_rate + rate AS last_rate_plus_rate,
    DATE_TRUNC('QUARTER', valid_to_year) AS valid_to_year_quarter
FROM screened
ORDER BY rate_filled DESC, valid_to_year_quarter DESC, id
LIMIT 10
