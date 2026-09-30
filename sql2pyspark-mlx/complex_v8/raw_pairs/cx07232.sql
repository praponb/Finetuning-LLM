WITH prepared AS (
    -- Scope the tariff_windows rows
    SELECT *
    FROM tariff_windows
    WHERE tariff IN ('EV', 'TOU')
      AND valid_to >= TIMESTAMP '2025-01-25 21:00:00'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', valid_to) AS days_since_valid_to,
        ROUND(rate * 1.5, 2) AS rate_adj
    FROM prepared
),
rollup_base AS (
    -- Summarise per group
    SELECT
        YEAR(valid_from) AS valid_from_year,
        tariff,
        DATE_TRUNC('MONTH', valid_to) AS valid_to_month,
        MIN(rate_adj) AS min_rate_adj,
        COUNT(*) AS n_rows
    FROM annotated
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
scored AS (
    -- Label and score rows
    SELECT *, valid_to_month + INTERVAL 30 MINUTES AS valid_to_month_plus_30m
    FROM rollup_base
),
enriched AS (
    -- Derive helper columns
    SELECT *, TO_DATE(valid_to_month) AS valid_to_month_date
    FROM scored
)
-- Final projection
SELECT
    valid_from_year,
    tariff,
    valid_to_month,
    min_rate_adj,
    n_rows,
    valid_to_month_plus_30m,
    valid_to_month_date,
    ROUND(valid_from_year / NULLIF(min_rate_adj, 0), 3) AS valid_from_year_per_min_rate_adj
FROM enriched
ORDER BY valid_from_year DESC, valid_to_month_plus_30m DESC NULLS FIRST, tariff, valid_to_month
LIMIT 10
