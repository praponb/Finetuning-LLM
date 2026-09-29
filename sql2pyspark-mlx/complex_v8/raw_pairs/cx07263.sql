WITH cleaned AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(valid_from)) / 3600, 1) AS hours_valid_from_to_valid_to,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(valid_to)) / 3600, 1) AS hours_valid_to_to_valid_from,
        MONTH(valid_to) AS valid_to_month
    FROM tariff_windows
    WHERE tariff = 'TOU'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(hours_valid_to_to_valid_from) OVER (PARTITION BY MONTH(valid_from), tariff) AS valid_from_month_tariff_max_hours_valid_to_to_va,
        SUM(valid_to_month) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from) ORDER BY valid_to DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS valid_to_month_sum_3_prior,
        hours_valid_from_to_valid_to - LAG(hours_valid_from_to_valid_to) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from) ORDER BY valid_to DESC, id) AS hours_valid_from_to_valid_to_change
    FROM cleaned
),
totals AS (
    -- Summarise per group
    SELECT
        tariff,
        valid_to_month,
        YEAR(valid_from) AS valid_from_year,
        ROUND(AVG(hours_valid_from_to_valid_to_change), 2) AS avg_hours_valid_from_to_valid_to_change,
        COUNT(*) FILTER (WHERE tariff = 'FLAT') AS n_tariff_flat,
        MIN(valid_to) AS first_valid_to,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list,
        SUM(valid_to_month) AS total_valid_to_month
    FROM metrics
    WHERE rate BETWEEN 2.2 AND 5.9
    GROUP BY 1, 2, 3
)
SELECT *
FROM (
    SELECT valid_from_year, tariff, total_valid_to_month
    FROM totals
) src
PIVOT (
    MIN(total_valid_to_month)
    FOR tariff IN ('EV', 'TOU', 'FLAT')
)
ORDER BY valid_from_year
