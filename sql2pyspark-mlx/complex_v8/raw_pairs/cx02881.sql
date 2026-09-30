WITH cleaned_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        ROUND(rate * 1.5, 2) AS rate_adj,
        DATEDIFF(DATE '2025-12-31', valid_to) AS days_since_valid_to
    FROM tariff_windows
    WHERE MONTH(valid_from) IN (1, 2)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        valid_from,
        rate_adj,
        days_since_valid_to,
        LAG(valid_from, 3) OVER (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_from) ORDER BY valid_from ASC, id) AS prev3_valid_from,
        rate - LAG(rate) OVER (PARTITION BY tariff, TO_DATE(valid_from) ORDER BY valid_to, id) AS rate_change
    FROM cleaned_tariffs
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(rate_adj) OVER w_prev3_valid_from_month_valid AS prev3_valid_from_month_valid_from_year_max_rate_,
        ROUND(days_since_valid_to - AVG(days_since_valid_to) OVER (PARTITION BY MONTH(prev3_valid_from), YEAR(valid_from)), 2) AS days_since_valid_to_vs_prev3_valid_from_month_va,
        RANK() OVER (ORDER BY prev3_valid_from) AS prev3_valid_from_rank
    FROM sequenced
    WINDOW w_prev3_valid_from_month_valid AS (PARTITION BY MONTH(prev3_valid_from), YEAR(valid_from))
),
scored AS (
    -- Add calculated columns
    SELECT *, COALESCE(rate_change, 0.0) AS rate_change_filled
    FROM metrics
    WHERE prev3_valid_from < TIMESTAMP '2025-03-09 04:00:00'
)
SELECT
    id,
    valid_from,
    rate_adj,
    rate_change,
    days_since_valid_to_vs_prev3_valid_from_month_va,
    rate_change_filled,
    ROUND(rate_change_filled * 0.25, 2) AS rate_change_filled_adj
FROM scored
