WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_TRUNC('YEAR', end_date) AS end_date_year,
        CONCAT_WS('-', status, product) AS status_product_label,
        QUARTER(start_date) AS start_date_quarter
    FROM policies
    WHERE status NOT IN ('EXPIRED', 'LAPSED')
      AND start_date >= DATE '2024-04-03'
),
summary AS (
    -- Group-level totals
    SELECT
        TO_DATE(end_date_year) AS end_date_year_day,
        SUM(CASE WHEN status <> 'EXPIRED' THEN premium ELSE 0 END) AS premium_status_expired,
        ROUND(SUM(premium) / NULLIF(SUM(start_date_quarter), 0), 3) AS premium_to_start_date_quarter_ratio,
        COUNT(*) FILTER (WHERE product IN ('HOME', 'LIFE')) AS n_product_in_home_life,
        MIN(end_date) AS first_end_date
    FROM recent_policys
    GROUP BY 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY n_product_in_home_life ORDER BY premium_to_start_date_quarter_ratio ASC) AS premium_to_start_date_quarter_ratio_rank,
        premium_status_expired - LAG(premium_status_expired) OVER w_end_date_year_day_month_n_pr AS premium_status_expired_change,
        LAST_VALUE(first_end_date) OVER (PARTITION BY MONTH(end_date_year_day), n_product_in_home_life ORDER BY premium_to_start_date_quarter_ratio DESC NULLS FIRST, end_date_year_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_end_date
    FROM summary
    WINDOW w_end_date_year_day_month_n_pr AS (PARTITION BY MONTH(end_date_year_day), n_product_in_home_life ORDER BY premium_to_start_date_quarter_ratio DESC NULLS FIRST, end_date_year_day)
)
-- Final projection
SELECT
    end_date_year_day,
    premium_status_expired,
    premium_to_start_date_quarter_ratio,
    n_product_in_home_life,
    premium_to_start_date_quarter_ratio_rank,
    premium_status_expired_change,
    FLOOR(premium_status_expired / 5000) * 5000 AS premium_status_expired_bucket,
    DATE_TRUNC('MONTH', last_first_end_date) AS last_first_end_date_month
FROM metrics
WHERE last_first_end_date < DATE '2026-10-23'
ORDER BY premium_status_expired_change DESC, end_date_year_day
LIMIT 20
