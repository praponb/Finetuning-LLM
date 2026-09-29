WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ABS(premium - 2945.9) AS premium_distance,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM policies
    WHERE holder_id >= 6
      AND end_date < DATE '2026-08-31'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(start_date, 2) OVER (PARTITION BY status ORDER BY days_since_start_date DESC, id) AS next2_start_date,
        LAG(premium, 1, 0) OVER (PARTITION BY YEAR(start_date) ORDER BY days_since_start_date DESC, id) AS prev_premium,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY end_date, id) AS seq_no
    FROM base_policys
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY seq_no, MONTH(start_date) ORDER BY seq_no DESC NULLS FIRST, id) AS seq_no_decile,
        SUM(days_since_start_date) OVER (PARTITION BY seq_no, MONTH(start_date) ORDER BY seq_no DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS days_since_start_date_sum_2,
        ROUND(AVG(prev_premium) OVER (PARTITION BY seq_no, MONTH(start_date) ORDER BY seq_no DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS prev_premium_avg_2,
        SIZE(COLLECT_SET(product) OVER w_status) AS distinct_product_in_status
    FROM sequenced
    WINDOW w_status AS (PARTITION BY status)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, days_since_start_date, distinct_product_in_status, seq_no_decile, days_since_start_date_sum_2
    FROM metrics
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (days_since_start_date, distinct_product_in_status, seq_no_decile, days_since_start_date_sum_2)
) unp
