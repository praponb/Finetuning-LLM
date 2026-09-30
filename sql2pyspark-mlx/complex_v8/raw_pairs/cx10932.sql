WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        INITCAP(TRIM(product)) AS product_clean
    FROM policies
    WHERE holder_id IS NOT NULL
      AND product = 'HEALTH'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        product_clean,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status), 0), 4) AS premium_share_of_start_date_month_status,
        LEAD(premium) OVER (PARTITION BY YEAR(start_date) ORDER BY start_date DESC, id) AS next_premium,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY status ORDER BY premium, id)) AS days_since_prev_end_date,
        ROUND(premium / NULLIF(SUM(premium) OVER w_status, 0), 4) AS premium_share_of_status,
        ROUND(premium - AVG(premium) OVER w_start_date_month_status, 2) AS premium_vs_start_date_month_status_avg,
        LEAD(start_date) OVER w_start_date_year_by_start_dat AS next_start_date,
        MAX(premium) OVER (PARTITION BY status) AS status_max_premium,
        ROW_NUMBER() OVER (PARTITION BY YEAR(start_date) ORDER BY start_date DESC, id) AS rn,
        SUM(premium) OVER (PARTITION BY status) AS status_sum_premium,
        ROUND(AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status), 2) AS start_date_month_status_avg_premium,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status) AS rows_in_start_date_month_status,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY status)) AS distinct_product_in_status
    FROM recent_policys
    WINDOW w_status AS (PARTITION BY status), w_start_date_month_status AS (PARTITION BY DATE_TRUNC('MONTH', start_date), status), w_start_date_year_by_start_dat AS (PARTITION BY YEAR(start_date) ORDER BY start_date DESC, id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(next_premium, 2) OVER (PARTITION BY MONTH(next_start_date) ORDER BY premium_share_of_status DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next_premium,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY distinct_product_in_status ORDER BY rn DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        LAST_VALUE(next_premium) OVER (PARTITION BY distinct_product_in_status ORDER BY rn DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_next_premium,
        SUM(rn) OVER (PARTITION BY rn) AS rn_sum_rn,
        ROUND(AVG(status_max_premium) OVER w_rn, 2) AS rn_avg_status_max_premium,
        ROUND(rn / NULLIF(SUM(rn) OVER (PARTITION BY rn), 0), 4) AS rn_share_of_rn,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_product_in_status ORDER BY rn DESC NULLS FIRST), 3) AS rn_cume_dist
    FROM metrics
    WINDOW w_rn AS (PARTITION BY rn)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    end_date,
    product_clean,
    next_premium,
    days_since_prev_end_date,
    premium_share_of_status,
    premium_vs_start_date_month_status_avg,
    next_start_date,
    status_max_premium,
    rn,
    status_sum_premium,
    rows_in_start_date_month_status,
    distinct_product_in_status,
    second_next_premium,
    last_known_start_date,
    last_next_premium,
    rn_sum_rn,
    DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date
FROM sequenced
ORDER BY next_start_date DESC
