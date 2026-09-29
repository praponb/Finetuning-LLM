WITH cleaned AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        ROUND(rate * 1.5, 2) AS rate_adj,
        TO_DATE(valid_to) AS valid_to_date
    FROM tariff_windows
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(valid_to_date) AS valid_to_date_year,
        DATE_TRUNC('MONTH', valid_from) AS valid_from_month,
        COUNT(*) AS n_rows,
        MIN(rate) AS min_rate,
        MIN(valid_from) AS first_valid_from,
        MAX(valid_to) AS last_valid_to,
        ROUND(AVG(rate), 2) AS avg_rate
    FROM cleaned
    GROUP BY 1, 2
)
SELECT valid_to_date_year, valid_from_month, field, metric_value
FROM (
    SELECT valid_to_date_year, valid_from_month, min_rate, avg_rate
    FROM stats
) src
UNPIVOT (
    metric_value FOR field IN (min_rate, avg_rate)
) unp
